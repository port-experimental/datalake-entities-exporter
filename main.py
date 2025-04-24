import json
import asyncio
from typing import Dict, Any
from loguru import logger
import os
from dotenv import load_dotenv
from clients.port import PortClient
from clients.bigquery import BigQueryClient

load_dotenv()

async def load_config() -> Dict[str, Any]:
    with open("config.json", "r") as f:
        config_data = json.load(f)
        logger.debug(f"Loaded config: {json.dumps(config_data, indent=2)}")
        return config_data

async def export_blueprint(
    port_client: PortClient,
    bigquery_client: BigQueryClient,
    blueprint_config: Dict[str, Any]
) -> None:
    blueprint_identifier = blueprint_config["identifier"]
    search_query = blueprint_config["search_query"]
    
    logger.info(f"Exporting blueprint: {blueprint_identifier}")
    
    # Get blueprint schema
    blueprint = await port_client.get_blueprint(blueprint_identifier)
    logger.debug(f"Blueprint schema for {blueprint_identifier}: {json.dumps(blueprint.get('schema', {}), indent=2)}")
    schema = bigquery_client._create_schema_from_blueprint(blueprint)
    
    # Create or update table
    bigquery_client.create_or_update_table(blueprint_identifier, schema)
    
    # Search and export entities
    response = await port_client.search_entities(blueprint_identifier, search_query)
    entities = response.get("entities", [])
    
    if entities:
        logger.debug(f"Found {len(entities)} entities for {blueprint_identifier}")
        bigquery_client.insert_entities(blueprint_identifier, entities)
        logger.info(f"Exported {len(entities)} entities for blueprint {blueprint_identifier}")
    else:
        logger.info(f"No entities found for blueprint {blueprint_identifier}")

async def main():
    # Load configuration
    config_data = await load_config()
    
    # Initialize clients
    port_client = PortClient()
    bigquery_client = BigQueryClient(
        project_id=os.getenv("BIGQUERY_PROJECT_ID"),
        dataset_id=os.getenv("BIGQUERY_DATASET_ID"),
        auto_migrate=os.getenv("AUTO_MIGRATE", "weak")
    )
    
    # Export each blueprint
    for blueprint_config in config_data["blueprints"]:
        await export_blueprint(port_client, bigquery_client, blueprint_config)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        exit(1) 