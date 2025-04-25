import json
from typing import Any, Dict, List, Literal, Optional, Set

from google.cloud import bigquery
from google.oauth2 import service_account
from loguru import logger


class BigQueryClient:
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        credentials: service_account.Credentials,
        auto_migrate: Literal["weak", "balanced", "hard"] = "weak",
    ):
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        self.dataset_id = dataset_id
        self.dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        self.auto_migrate = auto_migrate.lower()
        if self.auto_migrate not in ["weak", "balanced", "hard"]:
            raise ValueError("auto_migrate must be one of: 'weak', 'balanced', 'hard'")

    def _map_port_type_to_bigquery(self, port_type: str, format: Optional[str] = None) -> str:
        # First check if it's a string with a specific format
        if port_type.lower() == "string" and format:
            format_mapping = {
                "url": "STRING",
                "email": "STRING",
                "markdown": "STRING",
                "user": "STRING",
                "date-time": "TIMESTAMP",
            }
            return format_mapping.get(format.lower(), "STRING")

        # Then check the base type
        type_mapping = {
            "string": "STRING",
            "number": "FLOAT64",
            "boolean": "BOOL",
            "array": "STRING",  # Storing arrays as JSON strings
            "object": "STRING",  # Storing objects as JSON strings
            "datetime": "TIMESTAMP",
        }
        return type_mapping.get(port_type.lower(), "STRING")

    def _create_schema_from_blueprint(self, blueprint: Dict[str, Any]) -> List[bigquery.SchemaField]:
        schema = [
            bigquery.SchemaField("identifier", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING"),
            bigquery.SchemaField("created_at", "TIMESTAMP"),
            bigquery.SchemaField("updated_at", "TIMESTAMP"),
        ]

        # Handle properties
        properties = blueprint.get("schema", {}).get("properties", {})
        required_properties = blueprint.get("schema", {}).get("required", [])

        for prop_name, prop_details in properties.items():
            field_type = self._map_port_type_to_bigquery(prop_details.get("type", "string"), prop_details.get("format"))
            mode = "REQUIRED" if prop_name in required_properties else "NULLABLE"
            schema.append(bigquery.SchemaField(prop_name, field_type, mode=mode))

        # Handle relations
        relations = blueprint.get("relations", {})
        for relation_name, relation_details in relations.items():
            is_many = relation_details.get("many", False)
            is_required = relation_details.get("required", False)

            if is_many:
                # For many relations, store as JSON string of identifiers
                schema.append(
                    bigquery.SchemaField(
                        relation_name,
                        "STRING",
                        mode="NULLABLE",
                        description=f"JSON array of {relation_name} identifiers",
                    )
                )
            else:
                # For single relations, store as string identifier
                mode = "REQUIRED" if is_required else "NULLABLE"
                schema.append(bigquery.SchemaField(relation_name, "STRING", mode=mode))

        # Handle calculation properties
        calculation_properties = blueprint.get("calculationProperties", {})
        for calc_name, calc_details in calculation_properties.items():
            field_type = self._map_port_type_to_bigquery(calc_details.get("type", "string"), calc_details.get("format"))
            schema.append(
                bigquery.SchemaField(
                    calc_name,
                    field_type,
                    mode="NULLABLE",
                    description=f"Calculated property: {calc_details.get('description', '')}",
                )
            )

        # Handle aggregation properties
        aggregation_properties = blueprint.get("aggregationProperties", {})
        for agg_name, agg_details in aggregation_properties.items():
            field_type = self._map_port_type_to_bigquery(agg_details.get("type", "string"), agg_details.get("format"))
            schema.append(
                bigquery.SchemaField(
                    agg_name,
                    field_type,
                    mode="NULLABLE",
                    description=f"Aggregation property: {agg_details.get('description', '')}",
                )
            )

        # Handle mirror properties
        mirror_properties = blueprint.get("mirrorProperties", {})
        for mirror_name, mirror_details in mirror_properties.items():
            field_type = "STRING"  # Mirror properties are always strings
            schema.append(
                bigquery.SchemaField(
                    mirror_name,
                    field_type,
                    mode="NULLABLE",
                    description=f"Mirror property from path: {mirror_details.get('path', '')}",
                )
            )

        return schema

    def _get_existing_schema_fields(self, table_ref: bigquery.TableReference) -> Set[str]:
        try:
            table = self.client.get_table(table_ref)
            fields = {field.name for field in table.schema}
            logger.debug(f"Existing schema fields for {table_ref.table_id}: {fields}")
            return fields
        except Exception as e:
            logger.debug(f"No existing table found for {table_ref.table_id}: {str(e)}")
            return set()

    def _get_new_schema_fields(self, schema: List[bigquery.SchemaField]) -> Set[str]:
        fields = {field.name for field in schema}
        logger.debug(f"New schema fields: {fields}")
        return fields

    def _compare_schemas(self, existing_fields: Set[str], new_fields: Set[str]) -> Dict[str, Set[str]]:
        changes = {
            "added": new_fields - existing_fields,
            "removed": existing_fields - new_fields if self.auto_migrate == "hard" else set(),
            "unchanged": existing_fields & new_fields,
        }
        logger.debug(f"Schema comparison results: {changes}")
        return changes

    def create_or_update_table(self, table_id: str, schema: List[bigquery.SchemaField]) -> None:
        table_ref = self.dataset_ref.table(table_id)
        logger.info(f"Processing table {table_id} in {self.auto_migrate} mode")

        if self.auto_migrate == "weak":
            try:
                table = self.client.get_table(table_ref)
                logger.info(f"Table {table_id} already exists, no changes made (weak mode)")
                return
            except Exception:
                table = bigquery.Table(table_ref, schema=schema)
                table = self.client.create_table(table)
                logger.info(f"Created table {table_id}")
                return

        try:
            existing_table = self.client.get_table(table_ref)
            logger.debug(f"Found existing table {table_id}")

            existing_fields = self._get_existing_schema_fields(table_ref)
            new_fields = self._get_new_schema_fields(schema)

            schema_changes = self._compare_schemas(existing_fields, new_fields)

            if not schema_changes["added"] and not schema_changes["removed"]:
                logger.info(f"Table {table_id} schema is up to date")
                return

            if schema_changes["added"]:
                logger.info(f"Adding new fields to {table_id}: {schema_changes['added']}")
                new_schema = existing_table.schema.copy()
                for field in schema:
                    if field.name in schema_changes["added"]:
                        logger.debug(f"Adding field {field.name} with type {field.field_type}")
                        new_schema.append(field)
                existing_table.schema = new_schema
                try:
                    self.client.update_table(existing_table, ["schema"])
                    logger.info(f"Successfully updated schema for {table_id}")
                except Exception as e:
                    logger.error(f"Failed to update schema for {table_id}: {str(e)}")
                    raise

            # Only remove fields if in hard mode
            if self.auto_migrate == "hard" and schema_changes["removed"]:
                logger.info(f"Removing fields from {table_id}: {schema_changes['removed']}")
                new_schema = [field for field in existing_table.schema if field.name not in schema_changes["removed"]]
                existing_table.schema = new_schema
                try:
                    self.client.update_table(existing_table, ["schema"])
                    logger.info(f"Successfully removed fields from {table_id}")
                except Exception as e:
                    logger.error(f"Failed to remove fields from {table_id}: {str(e)}")
                    raise
            elif schema_changes["removed"]:
                logger.info(f"Fields would be removed in hard mode: {schema_changes['removed']}")

        except Exception as e:
            logger.error(f"Error processing table {table_id}: {str(e)}")
            # Table doesn't exist, create it
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table)
            logger.info(f"Created new table {table_id}")

    def insert_entities(self, table_id: str, entities: list[dict[str, Any]]) -> None:
        table_ref = self.dataset_ref.table(table_id)
        table = self.client.get_table(table_ref)

        # Get all field names from the table schema
        schema_fields = {field.name for field in table.schema}

        # First, get all existing identifiers
        query = f"""
            SELECT identifier FROM `{table.project}.{table.dataset_id}.{table.table_id}`
        """
        existing_identifiers = set()
        try:
            query_job = self.client.query(query)
            for row in query_job:
                existing_identifiers.add(row.identifier)
        except Exception as e:
            logger.debug(f"No existing identifiers found or error querying: {str(e)}")

        rows_to_insert = []
        rows_to_update = []

        for entity in entities:
            row = {
                "identifier": entity["identifier"],
                "title": entity["title"],
                "created_at": entity.get("createdAt"),
                "updated_at": entity.get("updatedAt"),
            }

            # Add properties
            for prop_name, prop_value in entity.get("properties", {}).items():
                if prop_name in schema_fields:
                    row[prop_name] = prop_value

            # Add relations
            for relation_name, relation in entity.get("relations", {}).items():
                if relation_name in schema_fields:
                    if isinstance(relation, str):
                        # Single relation
                        row[relation_name] = relation
                    elif isinstance(relation, list):
                        # Many relations
                        row[relation_name] = json.dumps(relation)

            # Add calculation properties
            for calc_name, calc_value in entity.get("calculationProperties", {}).items():
                if calc_name in schema_fields:
                    row[calc_name] = calc_value

            # Add aggregation properties
            for agg_name, agg_value in entity.get("aggregationProperties", {}).items():
                if agg_name in schema_fields:
                    row[agg_name] = agg_value

            # Add mirror properties
            for mirror_name, mirror_value in entity.get("mirrorProperties", {}).items():
                if mirror_name in schema_fields:
                    row[mirror_name] = mirror_value

            if entity["identifier"] in existing_identifiers:
                rows_to_update.append(row)
            else:
                rows_to_insert.append(row)

        # Handle inserts
        if rows_to_insert:
            errors = self.client.insert_rows_json(table, rows_to_insert)
            if errors:
                logger.error(f"Errors while inserting rows: {errors}")
            else:
                logger.info(f"Successfully inserted {len(rows_to_insert)} rows into {table_id}")

        # Handle updates
        if rows_to_update:
            for row in rows_to_update:
                identifier = row.pop("identifier")
                update_query = f"""
                    UPDATE `{table.project}.{table.dataset_id}.{table.table_id}`
                    SET {', '.join(f"{k} = @{k}" for k in row.keys())}
                    WHERE identifier = @identifier
                """

                # Create a mapping of field names to their types
                field_types = {field.name: field.field_type for field in table.schema}

                # Create query parameters with correct types
                query_parameters = []
                for k, v in row.items():
                    field_type = field_types.get(k, "STRING")  # Default to STRING if type not found
                    if field_type == "TIMESTAMP" and isinstance(v, str):
                        # Convert string timestamps to datetime
                        from datetime import datetime

                        v = datetime.fromisoformat(v.replace("Z", "+00:00"))
                    query_parameters.append(bigquery.ScalarQueryParameter(k, field_type, v))
                query_parameters.append(bigquery.ScalarQueryParameter("identifier", "STRING", identifier))

                job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

                try:
                    query_job = self.client.query(update_query, job_config=job_config)
                    query_job.result()  # Wait for the query to complete
                except Exception as e:
                    logger.error(f"Error updating row with identifier {identifier}: {str(e)}")

            logger.info(f"Completed updates for {len(rows_to_update)} rows in {table_id}")
