import asyncio
import json
from typing import Any, Dict, List, Literal, Optional, Set

from google.cloud import bigquery
from google.oauth2 import credentials
from loguru import logger


class BigQueryClient:
    """Client for interacting with Google BigQuery.

    This client handles all BigQuery operations including table management,
    schema updates, and data insertion. It supports automatic schema migration
    with different modes of operation.

    Attributes:
        client: The BigQuery client instance.
        dataset_id: The ID of the BigQuery dataset.
        dataset_ref: Reference to the BigQuery dataset.
        auto_migrate: Schema migration mode ('weak', 'balanced', or 'hard').
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        credentials: credentials.Credentials,
        auto_migrate: Literal["weak", "balanced", "hard"] = "weak",
    ):
        """Initialize the BigQuery client.

        Args:
            project_id: Google Cloud project ID.
            dataset_id: BigQuery dataset ID.
            credentials: Google Cloud credentials.
            auto_migrate: Schema migration mode ('weak', 'balanced', or 'hard').
        """
        self.client = bigquery.Client(project=project_id, credentials=credentials)
        self.dataset_id = dataset_id
        self.dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
        self.auto_migrate = auto_migrate.lower()
        if self.auto_migrate not in ["weak", "balanced", "hard"]:
            raise ValueError("auto_migrate must be one of: 'weak', 'balanced', 'hard'")

    def _map_port_type_to_bigquery(self, port_type: str, format: Optional[str] = None) -> str:
        """Map Port entity types to BigQuery field types.

        Args:
            port_type: The Port entity type.
            format: Optional format specification for the type.

        Returns:
            Corresponding BigQuery field type.
        """
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

    def _create_property_fields(
        self, properties: Dict[str, Any], required_properties: List[str]
    ) -> List[bigquery.SchemaField]:
        """Create schema fields for Port properties.

        Args:
            properties: Dictionary of property definitions.
            required_properties: List of required property names (ignored).

        Returns:
            List of schema fields for properties.
        """
        fields = []
        for prop_name, prop_details in properties.items():
            field_type = self._map_port_type_to_bigquery(
                prop_details.get("type", "string"), prop_details.get("format")
            )
            # All fields are nullable to handle migrations easily
            fields.append(bigquery.SchemaField(prop_name, field_type, mode="NULLABLE"))
        return fields

    def _create_relation_fields(self, relations: Dict[str, Any]) -> List[bigquery.SchemaField]:
        """Create schema fields for Port relations.

        Args:
            relations: Dictionary of relation definitions.

        Returns:
            List of schema fields for relations.
        """
        fields = []
        for relation_name, relation_details in relations.items():
            is_many = relation_details.get("many", False)
            # All fields are nullable to handle migrations easily
            if is_many:
                fields.append(
                    bigquery.SchemaField(
                        relation_name,
                        "STRING",
                        mode="NULLABLE",
                        description=f"JSON array of {relation_name} identifiers",
                    )
                )
            else:
                fields.append(bigquery.SchemaField(relation_name, "STRING", mode="NULLABLE"))
        return fields

    def _create_calculation_fields(
        self, calculation_properties: Dict[str, Any]
    ) -> List[bigquery.SchemaField]:
        """Create schema fields for calculation properties.

        Args:
            calculation_properties: Dictionary of calculation property definitions.

        Returns:
            List of schema fields for calculation properties.
        """
        fields = []
        for calc_name, calc_details in calculation_properties.items():
            field_type = self._map_port_type_to_bigquery(
                calc_details.get("type", "string"), calc_details.get("format")
            )
            fields.append(
                bigquery.SchemaField(
                    calc_name,
                    field_type,
                    mode="NULLABLE",
                    description=f"Calculated property: {calc_details.get('description', '')}",
                )
            )
        return fields

    def _create_aggregation_fields(
        self, aggregation_properties: Dict[str, Any]
    ) -> List[bigquery.SchemaField]:
        """Create schema fields for aggregation properties.

        Args:
            aggregation_properties: Dictionary of aggregation property definitions.

        Returns:
            List of schema fields for aggregation properties.
        """
        fields = []
        for agg_name, agg_details in aggregation_properties.items():
            field_type = self._map_port_type_to_bigquery(
                agg_details.get("type", "string"), agg_details.get("format")
            )
            fields.append(
                bigquery.SchemaField(
                    agg_name,
                    field_type,
                    mode="NULLABLE",
                    description=f"Aggregation property: {agg_details.get('description', '')}",
                )
            )
        return fields

    def _create_mirror_fields(self, mirror_properties: Dict[str, Any]) -> List[bigquery.SchemaField]:
        """Create schema fields for mirror properties.

        Args:
            mirror_properties: Dictionary of mirror property definitions.

        Returns:
            List of schema fields for mirror properties.
        """
        fields = []
        for mirror_name, mirror_details in mirror_properties.items():
            fields.append(
                bigquery.SchemaField(
                    mirror_name,
                    "STRING",
                    mode="NULLABLE",
                    description=f"Mirror property from path: {mirror_details.get('path', '')}",
                )
            )
        return fields

    def _create_schema_from_blueprint(self, blueprint: Dict[str, Any]) -> List[bigquery.SchemaField]:
        """Create BigQuery schema from Port blueprint.

        Args:
            blueprint: Port blueprint containing schema information.

        Returns:
            List of BigQuery schema fields.
        """
        # All fields except identifier are nullable to handle migrations easily
        schema = [
            bigquery.SchemaField("identifier", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("title", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ]

        # Handle properties
        properties = blueprint.get("schema", {}).get("properties", {})
        required_properties = blueprint.get("schema", {}).get("required", [])
        schema.extend(self._create_property_fields(properties, required_properties))

        # Handle relations
        relations = blueprint.get("relations", {})
        schema.extend(self._create_relation_fields(relations))

        # Handle calculation properties
        calculation_properties = blueprint.get("calculationProperties", {})
        schema.extend(self._create_calculation_fields(calculation_properties))

        # Handle aggregation properties
        aggregation_properties = blueprint.get("aggregationProperties", {})
        schema.extend(self._create_aggregation_fields(aggregation_properties))

        # Handle mirror properties
        mirror_properties = blueprint.get("mirrorProperties", {})
        schema.extend(self._create_mirror_fields(mirror_properties))

        return schema

    async def _get_existing_schema_fields(self, table_ref: bigquery.TableReference) -> Set[str]:
        """Get existing schema fields from a BigQuery table.

        Args:
            table_ref: Reference to the BigQuery table.

        Returns:
            Set of existing field names.
        """
        try:
            table = await asyncio.to_thread(self.client.get_table, table_ref)
            fields = {field.name for field in table.schema}
            logger.debug(f"Existing schema fields for {table_ref.table_id}: {fields}")
            return fields
        except Exception as e:
            logger.debug(f"No existing table found for {table_ref.table_id}: {str(e)}")
            return set()

    def _get_new_schema_fields(self, schema: List[bigquery.SchemaField]) -> Set[str]:
        """Get field names from a new schema.

        Args:
            schema: List of BigQuery schema fields.

        Returns:
            Set of field names.
        """
        fields = {field.name for field in schema}
        logger.debug(f"New schema fields: {fields}")
        return fields

    def _compare_schemas(self, existing_fields: Set[str], new_fields: Set[str]) -> Dict[str, Set[str]]:
        """Compare existing and new schema fields.

        Args:
            existing_fields: Set of existing field names.
            new_fields: Set of new field names.

        Returns:
            Dictionary containing sets of added, removed, and unchanged fields.
        """
        changes = {
            "added": new_fields - existing_fields,
            "removed": existing_fields - new_fields if self.auto_migrate == "hard" else set(),
            "unchanged": existing_fields & new_fields,
        }
        logger.debug(f"Schema comparison results: {changes}")
        return changes

    async def create_or_update_table(self, table_id: str, schema: List[bigquery.SchemaField]) -> None:
        """Create or update a BigQuery table with the given schema.

        Args:
            table_id: ID of the table to create or update.
            schema: List of BigQuery schema fields.
        """
        table_ref = self.dataset_ref.table(table_id)
        logger.info(f"Processing table {table_id} in {self.auto_migrate} mode")

        if self.auto_migrate == "weak":
            try:
                table = await asyncio.to_thread(self.client.get_table, table_ref)
                logger.info(f"Table {table_id} already exists, no changes made (weak mode)")
                return
            except Exception:
                table = bigquery.Table(table_ref, schema=schema)
                table = await asyncio.to_thread(self.client.create_table, table)
                logger.info(f"Created table {table_id}")
                return

        try:
            existing_table = await asyncio.to_thread(self.client.get_table, table_ref)
            logger.debug(f"Found existing table {table_id}")

            existing_fields = await self._get_existing_schema_fields(table_ref)
            new_fields = self._get_new_schema_fields(schema)

            schema_changes = self._compare_schemas(existing_fields, new_fields)

            if not schema_changes["added"] and not schema_changes["removed"]:
                logger.info(f"Table {table_id} schema is up to date")
                return

            if schema_changes["added"]:
                await self._add_fields_to_table(existing_table, schema, schema_changes["added"])

            if self.auto_migrate == "hard" and schema_changes["removed"]:
                await self._remove_fields_from_table(existing_table, schema_changes["removed"])
            elif schema_changes["removed"]:
                logger.info(f"Fields would be removed in hard mode: {schema_changes['removed']}")

        except Exception as e:
            logger.error(f"Error processing table {table_id}: {str(e)}")
            # Table doesn't exist, create it
            table = bigquery.Table(table_ref, schema=schema)
            table = await asyncio.to_thread(self.client.create_table, table)
            logger.info(f"Created new table {table_id}")

    async def _add_fields_to_table(
        self, table: bigquery.Table, schema: List[bigquery.SchemaField], fields_to_add: Set[str]
    ) -> None:
        """Add new fields to an existing table.

        Args:
            table: The BigQuery table to update.
            schema: List of all schema fields.
            fields_to_add: Set of field names to add.
        """
        logger.info(f"Adding new fields to {table.table_id}: {fields_to_add}")
        new_schema = table.schema.copy()
        for field in schema:
            if field.name in fields_to_add:
                logger.debug(f"Adding field {field.name} with type {field.field_type}")
                new_schema.append(field)
        table.schema = new_schema
        try:
            await asyncio.to_thread(self.client.update_table, table, ["schema"])
            logger.info(f"Successfully updated schema for {table.table_id}")
        except Exception as e:
            logger.error(f"Failed to update schema for {table.table_id}: {str(e)}")
            raise

    async def _remove_fields_from_table(self, table: bigquery.Table, fields_to_remove: Set[str]) -> None:
        """Remove fields from an existing table.

        Args:
            table: The BigQuery table to update.
            fields_to_remove: Set of field names to remove.
        """
        logger.info(f"Removing fields from {table.table_id}: {fields_to_remove}")
        new_schema = [field for field in table.schema if field.name not in fields_to_remove]
        table.schema = new_schema
        try:
            await asyncio.to_thread(self.client.update_table, table, ["schema"])
            logger.info(f"Successfully removed fields from {table.table_id}")
        except Exception as e:
            logger.error(f"Failed to remove fields from {table.table_id}: {str(e)}")
            raise

    async def _get_existing_identifiers(self, table: bigquery.Table) -> Set[str]:
        """Get existing entity identifiers from a table.

        Args:
            table: The BigQuery table to query.

        Returns:
            Set of existing entity identifiers.
        """
        query = f"""
            SELECT identifier FROM `{table.project}.{table.dataset_id}.{table.table_id}`
        """
        existing_identifiers = set()
        try:
            query_job = await asyncio.to_thread(self.client.query, query)
            for row in query_job:
                existing_identifiers.add(row.identifier)
        except Exception as e:
            logger.debug(f"No existing identifiers found or error querying: {str(e)}")
        return existing_identifiers

    def _prepare_entity_row(
        self, entity: Dict[str, Any], schema_fields: Set[str]
    ) -> Dict[str, Any]:
        """Prepare a single entity for insertion into BigQuery.

        Args:
            entity: The entity data to prepare.
            schema_fields: Set of valid schema field names.

        Returns:
            Dictionary representing the row to insert.
        """
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
                    row[relation_name] = relation
                elif isinstance(relation, list):
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

        return row

    async def _execute_bulk_update(
        self, table: bigquery.Table, rows_to_update: List[Dict[str, Any]]
    ) -> None:
        """Execute a bulk update for multiple rows.

        Args:
            table: The BigQuery table to update.
            rows_to_update: List of rows to update, each containing an identifier and field values.
        """
        if not rows_to_update:
            return

        # Group rows by their field sets to create more efficient UPDATE statements
        field_sets: Dict[frozenset[str], List[tuple[str, Dict[str, Any]]]] = {}
        for row in rows_to_update:
            identifier = row.pop("identifier")
            field_set = frozenset(row.keys())
            if field_set not in field_sets:
                field_sets[field_set] = []
            field_sets[field_set].append((identifier, row))

        # Create a mapping of field names to their types
        field_types = {field.name: field.field_type for field in table.schema}

        async def _execute_single_update(identifier: str, row: Dict[str, Any], fields: List[str]) -> None:
            """Execute a single update query.

            Args:
                identifier: The entity identifier to update.
                row: Dictionary of field values to update.
                fields: List of fields being updated.
            """
            update_query = f"""
                UPDATE `{table.project}.{table.dataset_id}.{table.table_id}`
                SET {', '.join(f"{field} = @{field}" for field in fields)}
                WHERE identifier = @identifier
            """

            query_parameters = []
            for field in fields:
                field_type = field_types.get(field, "STRING")
                value = row[field]
                if field_type == "TIMESTAMP" and isinstance(value, str):
                    from datetime import datetime
                    value = datetime.fromisoformat(value.replace("Z", "+00:00"))
                query_parameters.append(bigquery.ScalarQueryParameter(field, field_type, value))
            query_parameters.append(bigquery.ScalarQueryParameter("identifier", "STRING", identifier))

            job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

            try:
                query_job = await asyncio.to_thread(self.client.query, update_query, job_config)
                await asyncio.to_thread(query_job.result)
            except Exception as e:
                logger.error(f"Error updating row with identifier {identifier}: {str(e)}")

        # Execute updates for each field set in parallel
        for field_set, rows in field_sets.items():
            fields = list(field_set)
            # Create tasks for all rows in this field set
            tasks = [
                _execute_single_update(identifier, row, fields)
                for identifier, row in rows
            ]
            # Execute all updates for this field set in parallel
            await asyncio.gather(*tasks, return_exceptions=True)

    async def insert_entities(self, table_id: str, entities: list[dict[str, Any]]) -> None:
        """Insert or update entities in a BigQuery table.

        Args:
            table_id: ID of the table to insert into.
            entities: List of entities to insert or update.
        """
        table_ref = self.dataset_ref.table(table_id)
        table = await asyncio.to_thread(self.client.get_table, table_ref)

        # Get all field names from the table schema
        schema_fields = {field.name for field in table.schema}

        # Get existing identifiers
        existing_identifiers = await self._get_existing_identifiers(table)

        rows_to_insert = []
        rows_to_update = []

        # Prepare rows for insertion or update
        for entity in entities:
            row = self._prepare_entity_row(entity, schema_fields)

            if entity["identifier"] in existing_identifiers:
                rows_to_update.append(row)
            else:
                rows_to_insert.append(row)

        logger.info(f"Inserting {len(rows_to_insert)} rows and updating {len(rows_to_update)} rows")

        # Execute inserts and updates in parallel
        insert_task = asyncio.to_thread(self.client.insert_rows_json, table, rows_to_insert)
        update_task = self._execute_bulk_update(table, rows_to_update)

        # Wait for both operations to complete
        insert_errors, _ = await asyncio.gather(
            insert_task,
            update_task,
            return_exceptions=True
        )

        if insert_errors:
            logger.error(f"Errors while inserting rows: {insert_errors}")
        else:
            logger.info(f"Successfully inserted {len(rows_to_insert)} rows into {table_id}")

        logger.info(f"Completed updates for {len(rows_to_update)} rows in {table_id}")
