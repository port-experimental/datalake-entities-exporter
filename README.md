# Port to BigQuery Exporter

This tool exports entities from Port to BigQuery, automatically creating tables that mirror the schema of your Port blueprints.

## Setup

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
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to your Google Cloud service account key file
  OR
- `GOOGLE_APPLICATION_CREDENTIALS_JSON`: Your Google Cloud service account key as JSON string

### Google Cloud Authentication

You need to set up Google Cloud authentication to access BigQuery. There are two ways to do this:

1. **Service Account Key File**:
   - Create a service account in Google Cloud Console
   - Download the service account key as a JSON file
   - Set the `GOOGLE_APPLICATION_CREDENTIALS` environment variable to the path of this file

2. **Service Account Key JSON**:
   - Create a service account in Google Cloud Console
   - Copy the service account key JSON
   - Set the `GOOGLE_APPLICATION_CREDENTIALS_JSON` environment variable with the JSON content

The service account needs the following roles:
- BigQuery Data Editor
- BigQuery Job User

Note: You only need to set one of these environment variables, not both. If both are set, `GOOGLE_APPLICATION_CREDENTIALS_JSON` takes precedence.

4. Configure blueprints to export in `config.json`:
```json
{
    "blueprints": [
        {
            "identifier": "service",
            "search_query": {
                "combinator": "and",
                "rules": []
            }
        }
    ]
}
```

## Usage

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
1. Read the configuration from `config.json`
2. For each blueprint:
   - Fetch the blueprint schema from Port
   - Create or update a BigQuery table with matching schema based on AUTO_MIGRATE mode
   - Search for entities using the configured query
   - Export the entities to BigQuery
   - Handle streaming buffer conflicts automatically with retries

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
- `ruff` for linting
- `mypy` for type checking
- `black` for code formatting
- `pytest` for testing

To run these tools:
```bash
# Run linting
make lint

# Run type checking
make typecheck

# Run tests
make test

# Run all checks (lint, typecheck, test)
make check
```

## Features

- Automatic schema mapping from Port to BigQuery
- Support for all Port data types
- Configurable search queries for each blueprint
- Automatic table creation and updates with configurable migration modes
- Automatic handling of BigQuery streaming buffer conflicts
- Logging of export progress and errors

## Notes

- The script requires Google Cloud credentials to be set up in your environment
- Tables are created with the same name as the blueprint identifier
- Arrays and objects are stored as JSON strings in BigQuery
- The script maintains the original data types where possible
- When using balanced or hard modes, the script will automatically handle streaming buffer conflicts with retries
- Updates to existing rows may be delayed due to BigQuery's streaming buffer 