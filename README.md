# Port to BigQuery Exporter

This tool exports entities from Port to BigQuery, automatically creating tables that mirror the schema of your Port blueprints.

## Setup

1. Install Poetry if you haven't already:
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. Install dependencies:
```bash
poetry install
```

3. Configure environment variables in `.env`:
- `PORT_CLIENT_ID`: Your Port API client ID
- `PORT_CLIENT_SECRET`: Your Port API client secret
- `PORT_API_URL`: Port API URL (default: https://api.getport.io/v1)
- `BIGQUERY_PROJECT_ID`: Your Google Cloud project ID
- `BIGQUERY_DATASET_ID`: BigQuery dataset ID where tables will be created
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
poetry run python main.py
```

The script will:
1. Read the configuration from `config.json`
2. For each blueprint:
   - Fetch the blueprint schema from Port
   - Create or update a BigQuery table with matching schema
   - Search for entities using the configured query
   - Export the entities to BigQuery

## Development

The project uses several development tools:
- `ruff` for linting
- `mypy` for type checking
- `black` for code formatting
- `pytest` for testing

To run these tools:
```bash
# Run linting
poetry run ruff check .

# Run type checking
poetry run mypy .

# Run tests
poetry run pytest
```

## Features

- Automatic schema mapping from Port to BigQuery
- Support for all Port data types
- Configurable search queries for each blueprint
- Automatic table creation and updates
- Logging of export progress and errors

## Notes

- The script requires Google Cloud credentials to be set up in your environment
- Tables are created with the same name as the blueprint identifier
- Arrays and objects are stored as JSON strings in BigQuery
- The script maintains the original data types where possible 