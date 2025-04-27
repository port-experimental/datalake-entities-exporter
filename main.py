import asyncio
import json

from loguru import logger

from clients.bigquery import BigQueryClient
from clients.port import PortClient
from settings import BlueprintConfig, settings


async def export_blueprint(
    port_client: PortClient, bigquery_client: BigQueryClient, blueprint_config: BlueprintConfig
) -> None:
    blueprint_identifier = blueprint_config.identifier
    search_query = blueprint_config.search_query

    logger.info(f"Exporting blueprint: {blueprint_identifier}")

    # Get blueprint schema
    blueprint = await port_client.get_blueprint(blueprint_identifier)
    logger.debug(f"Blueprint schema for {blueprint_identifier}: {json.dumps(blueprint.get('schema', {}), indent=2)}")
    schema = bigquery_client._create_schema_from_blueprint(blueprint)

    # Create or update table
    await bigquery_client.create_or_update_table(blueprint_identifier, schema)

    # Search and export entities
    total_entities = 0
    async for entities in port_client.search_entities(
        blueprint_identifier,
        search_query.model_dump(),
        include_entities=blueprint_config.include_entities,
        exclude_entities=blueprint_config.exclude_entities,
    ):
        if entities:
            logger.debug(f"Found {len(entities)} entities for {blueprint_identifier}")
            await bigquery_client.insert_entities(blueprint_identifier, entities)
            total_entities += len(entities)
            logger.info(f"Exported {total_entities} entities so far for blueprint {blueprint_identifier}")

    if total_entities > 0:
        logger.info(f"Completed export of {total_entities} entities for blueprint {blueprint_identifier}")
    else:
        logger.info(f"No entities found for blueprint {blueprint_identifier}")


async def main() -> None:
    # Initialize clients
    port_client = PortClient(
        port_client_id=settings.PORT_CLIENT_ID,
        port_client_secret=settings.PORT_CLIENT_SECRET,
        port_api_url=settings.PORT_API_URL,
    )

    # Initialize BigQuery client with settings
    bigquery_client = BigQueryClient(
        project_id=settings.BIGQUERY_PROJECT_ID,
        dataset_id=settings.BIGQUERY_DATASET_ID,
        auto_migrate=settings.AUTO_MIGRATE,
        credentials=settings.get_google_credentials(),
    )

    # Get entities config
    config_data = settings.get_entities_config()

    # Export each blueprint
    for blueprint_config in config_data.blueprints:
        await export_blueprint(port_client, bigquery_client, blueprint_config)

    # Clean up duplicates after all blueprints are processed
    logger.info("Cleaning up duplicate rows in all tables...")
    for blueprint_config in config_data.blueprints:
        await bigquery_client.cleanup_duplicates(blueprint_config.identifier)
    logger.info("Completed cleanup of all tables")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    # except Exception as e:
    #     logger.error(f"Fatal error: {str(e)}")
    #     exit(1)
