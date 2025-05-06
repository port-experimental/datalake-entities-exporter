# Port to BigQuery Exporter

This tool exports entities from Port to BigQuery, automatically creating tables that mirror the schema of your Port blueprints.

## Setup

### Using Docker (Recommended)

1. Pull the Docker image:
```bash
docker pull ghcr.io/port-experimental/datalake-entities-exporter:0.1.0
```

2. Create configuration files in a directory:

   a. Create an entities config file (e.g., `entities_config.json`):
   ```json
   {
       "blueprints": [
           {
               "identifier": "service",
               "search_query": {
                   "combinator": "and",
                   "rules": []
               },
               "include_entities": ["entity1", "entity2"],
               "exclude_entities": ["entity3", "entity4"]
           }
       ]
   }
   ```

   b. Create a Google Cloud service account key file (e.g., `google_credentials.json`)

3. In the same directory as the configuration files, run the container:

```bash

docker run -it \
  -v $(pwd)/entities_config.json:/app/.config/entities_config.json \
  -v $(pwd)/google_credentials.json:/app/.config/google_credentials.json \
  -e PORT_CLIENT_ID=your_client_id \
  -e PORT_CLIENT_SECRET=your_client_secret \
  -e PORT_API_URL=https://api.getport.io/v1 \
  -e BIGQUERY_PROJECT_ID=your_project_id \
  -e BIGQUERY_DATASET_ID=your_dataset_id \
  -e AUTO_MIGRATE=weak \
  -e ENTITIES_CONFIG=/app/.config/entities_config.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/.config/google_credentials.json \
  ghcr.io/port-experimental/datalake-entities-exporter:latest
```

### Manual Setup (Alternative)

1. Install Poetry if you haven't already:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Install dependencies:
```bash
make install
```

3. Configure environment variables in `.env`:
- `PORT_CLIENT_ID`: Your Port API client ID
- `PORT_CLIENT_SECRET`: Your Port API client secret
- `PORT_API_URL`: Port API URL (default: https://api.getport.io/v1)
- `BIGQUERY_PROJECT_ID`: Your Google Cloud project ID
- `BIGQUERY_DATASET_ID`: BigQuery dataset ID where tables will be created
- `AUTO_MIGRATE`: Schema migration mode (default: "weak")
  - `weak`: No schema changes to existing tables
  - `balanced`: Adds new fields to existing tables
  - `hard`: Adds new fields and removes fields not in the new schema
- `ENTITIES_CONFIG_JSON`: Entities configuration as a JSON string
- `ENTITIES_CONFIG`: Path to the entities configuration file
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to the Google Cloud service account key file
- `GOOGLE_APPLICATION_CREDENTIALS_JSON`: Google Cloud service account key file as a JSON string

### Entities Configuration

You can provide the entities configuration in two ways:

1. **JSON String**:
   - Set the `ENTITIES_CONFIG_JSON` environment variable with the configuration as a JSON string
   ```json
   {
       "blueprints": [
           {
               "identifier": "service",
               "search_query": {
                   "combinator": "and",
                   "rules": []
               },
               "include_entities": ["entity1", "entity2"],
               "exclude_entities": ["entity3", "entity4"]
           }
       ]
   }
   ```

2. **Config File**:
   - Set the `ENTITIES_CONFIG` environment variable to the path of your config file
   - The file should contain the same JSON structure as above

Note: You only need to set one of these environment variables, not both. If both are set, `ENTITIES_CONFIG_JSON` takes precedence.

#### Blueprint Configuration Options

Each blueprint in the configuration can have the following options:

- `identifier`: The identifier of the blueprint to export
- `search_query`: The search query to filter entities
  - `combinator`: Either "and" or "or" to combine rules
  - `rules`: List of search rules to apply
- `include_entities`: (Optional) List of entity identifiers to include in the export
- `exclude_entities`: (Optional) List of entity identifiers to exclude from the export

Note: If both `include_entities` and `exclude_entities` are specified, the `exclude_entities` list will be applied after the `include_entities` list.

### Google Cloud Authentication

You need to set up Google Cloud authentication to access BigQuery. There are several ways to do this:

1. **Using Google Cloud SDK** (Recommended):
   ```bash
   # Install the Google Cloud SDK if you haven't already
   # https://cloud.google.com/sdk/docs/install

   # Login to your Google Cloud account
   gcloud auth login

   # Set your project ID
   gcloud config set project YOUR_PROJECT_ID

   # Create a service account
   gcloud iam service-accounts create datalake-exporter \
     --display-name "Port Datalake Exporter"

   # Grant necessary roles to the service account
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:datalake-exporter@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataEditor"
   gcloud projects add-iam-policy-binding YOUR_PROJECT_ID \
     --member="serviceAccount:datalake-exporter@YOUR_PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.jobUser"

   # Generate and download the service account key
   gcloud iam service-accounts keys create google_credentials.json \
     --iam-account=datalake-exporter@YOUR_PROJECT_ID.iam.gserviceaccount.com
   ```

2. **Service Account Key File**:
   - Create a service account in Google Cloud Console
   - Download the service account key as a JSON file
   - Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of this file

3. **Service Account Key JSON**:
   - Create a service account in Google Cloud Console
   - Copy the service account key JSON
   - Set the `GOOGLE_APPLICATION_CREDENTIALS_JSON` environment variable with the JSON content

The service account needs the following roles:
- BigQuery Data Editor
- BigQuery Job User

Note: You only need to use one of these authentication methods. If using the Google Cloud SDK method, the generated `google_credentials.json` file can be used directly with the Docker container or set as `GOOGLE_APPLICATION_CREDENTIALS`.

## Usage

### Using Docker

Run the exporter with different migration modes:
```bash
# Default mode (weak)
docker run -it \
  -v $(pwd)/entities_config.json:/app/config/entities_config.json \
  -v $(pwd)/google_credentials.json:/app/config/google_credentials.json \
  -e PORT_CLIENT_ID=your_client_id \
  -e PORT_CLIENT_SECRET=your_client_secret \
  -e BIGQUERY_PROJECT_ID=your_project_id \
  -e BIGQUERY_DATASET_ID=your_dataset_id \
  -e AUTO_MIGRATE=weak \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/config/google_credentials.json \
  -e ENTITIES_CONFIG=/app/config/entities_config.json \
  ghcr.io/port-experimental/datalake-entities-exporter:0.1.0

# Balanced mode - adds new fields but preserves existing ones
docker run -it \
  -v $(pwd)/entities_config.json:/app/config/entities_config.json \
  -v $(pwd)/google_credentials.json:/app/config/google_credentials.json \
  -e PORT_CLIENT_ID=your_client_id \
  -e PORT_CLIENT_SECRET=your_client_secret \
  -e BIGQUERY_PROJECT_ID=your_project_id \
  -e BIGQUERY_DATASET_ID=your_dataset_id \
  -e AUTO_MIGRATE=balanced \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/config/google_credentials.json \
  -e ENTITIES_CONFIG=/app/config/entities_config.json \
  ghcr.io/port-experimental/datalake-entities-exporter:0.1.0

# Hard mode - matches schema exactly, removing fields not in new schema
docker run -it \
  -v $(pwd)/entities_config.json:/app/config/entities_config.json \
  -v $(pwd)/google_credentials.json:/app/config/google_credentials.json \
  -e PORT_CLIENT_ID=your_client_id \
  -e PORT_CLIENT_SECRET=your_client_secret \
  -e BIGQUERY_PROJECT_ID=your_project_id \
  -e BIGQUERY_DATASET_ID=your_dataset_id \
  -e AUTO_MIGRATE=hard \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/config/google_credentials.json \
  -e ENTITIES_CONFIG=/app/config/entities_config.json \
  ghcr.io/port-experimental/datalake-entities-exporter:0.1.0
```

### Manual Usage

Run the exporter:
```bash
# Default mode (weak)
make run

# Balanced mode - adds new fields but preserves existing ones
AUTO_MIGRATE=balanced make run

# Hard mode - matches schema exactly, removing fields not in new schema
AUTO_MIGRATE=hard make run
```

The script will:
1. Read the configuration from either the JSON string or config file
2. For each blueprint:
   - Fetch the blueprint schema from Port
   - Create or update a BigQuery table with matching schema based on AUTO_MIGRATE mode
   - Search for entities using the configured query
   - Export the entities to BigQuery, handling pagination automatically
   - Show progress updates during export

## Schema Migration Modes

The exporter supports three schema migration modes:

1. **Weak Mode** (default):
   - No schema changes to existing tables
   - Tables are only created if they don't exist
   - Safest option, maintains backward compatibility

2. **Balanced Mode**:
   - Adds new fields to existing tables
   - Preserves existing fields even if they're not in the new schema
   - Good for gradual schema evolution

3. **Hard Mode**:
   - Adds new fields to existing tables
   - Removes fields that are not in the new schema
   - Most aggressive option, use with caution

## Development

The project uses several development tools:
- `ruff` for linting and code formatting
- `mypy` for static type checking

To run these tools:

```bash
# Run linting and type checking
make lint
```

## Features

- Automatic schema mapping from Port to BigQuery
- Support for all Port data types
- Configurable search queries for each blueprint
- Automatic table creation and updates with configurable migration modes
- Automatic pagination handling for large result sets
- Progress updates during export
- Logging of export progress and errors
- Automatic cleanup of duplicate rows after export
- Support for both service account and user credentials for Google Cloud authentication
- Environment variable validation using Pydantic
- Async/await support for better performance with large datasets
