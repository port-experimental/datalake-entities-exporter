from pathlib import Path
from typing import Any, Literal, cast

from dotenv import load_dotenv
from google.auth import exceptions
from google.oauth2 import credentials, service_account
from pydantic import BaseModel
from pydantic_settings import BaseSettings

# Load environment variables from .env file if it exists
load_dotenv()


class SearchQuery(BaseModel):
    combinator: Literal["and", "or"] = "and"
    rules: list[dict[str, Any]] = []


class BlueprintConfig(BaseModel):
    identifier: str
    search_query: SearchQuery
    include_entities: list[str] | None = None
    exclude_entities: list[str] | None = None


class EntitiesConfig(BaseModel):
    blueprints: list[BlueprintConfig]


class Settings(BaseSettings):
    # Port configuration
    PORT_CLIENT_ID: str
    PORT_CLIENT_SECRET: str
    PORT_API_URL: str = "https://api.getport.io/v1"

    # Entities configuration (either as JSON string or file path)
    ENTITIES_CONFIG_JSON: EntitiesConfig | None = None
    ENTITIES_CONFIG: Path | None = None

    # BigQuery configuration
    BIGQUERY_PROJECT_ID: str
    BIGQUERY_DATASET_ID: str
    AUTO_MIGRATE: Literal["weak", "balanced", "hard"] = "weak"

    # Google Cloud credentials (either JSON string or file path)
    GOOGLE_APPLICATION_CREDENTIALS_JSON: str | None = None
    GOOGLE_APPLICATION_CREDENTIALS: str | None = None

    def get_google_credentials(self) -> credentials.Credentials:
        if self.GOOGLE_APPLICATION_CREDENTIALS_JSON:
            return cast(
                credentials.Credentials,
                service_account.Credentials.from_service_account_info(self.GOOGLE_APPLICATION_CREDENTIALS_JSON),
            )
        elif self.GOOGLE_APPLICATION_CREDENTIALS:
            try:
                return cast(
                    credentials.Credentials,
                    service_account.Credentials.from_service_account_file(self.GOOGLE_APPLICATION_CREDENTIALS),
                )
            except exceptions.MalformedError:
                pass

            return cast(
                credentials.Credentials,
                credentials.Credentials.from_authorized_user_file(self.GOOGLE_APPLICATION_CREDENTIALS),
            )

        else:
            raise ValueError(
                "Either GOOGLE_APPLICATION_CREDENTIALS or GOOGLE_APPLICATION_CREDENTIALS_JSON "
                "must be set in the environment"
            )

    def get_entities_config(self) -> EntitiesConfig:
        if self.ENTITIES_CONFIG_JSON:
            return self.ENTITIES_CONFIG_JSON
        elif self.ENTITIES_CONFIG:
            with open(self.ENTITIES_CONFIG, "r") as f:
                return EntitiesConfig.model_validate_json(f.read())
        else:
            raise ValueError("Either ENTITIES_CONFIG_JSON or ENTITIES_CONFIG " "must be set in the environment")


settings = Settings()  # type: ignore
