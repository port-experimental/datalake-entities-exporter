from datetime import datetime, timedelta
from typing import Any, AsyncGenerator

import httpx
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


class PortClient:
    def __init__(self, port_client_id: str, port_client_secret: str, port_api_url: str = "https://api.getport.io/v1"):
        self.port_client_id = port_client_id
        self.port_client_secret = port_client_secret
        self.port_api_url = port_api_url
        self.port_access_token: str | None = None
        self.token_expiry_time: datetime = datetime.now()
        self.port_headers: dict[str, str] = {}
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(60))

    async def get_access_token(self) -> tuple[str, datetime]:
        credentials = {"clientId": self.port_client_id, "clientSecret": self.port_client_secret}
        token_response = await self.client.post(f"{self.port_api_url}/auth/access_token", json=credentials)
        token_response.raise_for_status()
        response_data = token_response.json()
        access_token = response_data["accessToken"]
        expires_in = response_data["expiresIn"]
        token_expiry_time = datetime.now() + timedelta(seconds=expires_in)
        return access_token, token_expiry_time

    async def refresh_access_token(self) -> None:
        logger.info("Refreshing access token...")
        self.port_access_token, self.token_expiry_time = await self.get_access_token()
        self.port_headers = {"Authorization": f"Bearer {self.port_access_token}"}
        logger.info("New token received")

    async def refresh_token_if_expired(self) -> None:
        if datetime.now() >= self.token_expiry_time:
            await self.refresh_access_token()

    async def search_entities(
        self,
        blueprint_identifier: str,
        search_query: dict[str, Any],
        include_entities: list[str] | None = None,
        exclude_entities: list[str] | None = None,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        await self.refresh_token_if_expired()
        url = f"{self.port_api_url}/blueprints/{blueprint_identifier}/entities/search"

        # Start with the initial search query
        current_query = search_query.copy()

        while True:
            # Prepare the request payload
            payload: dict[str, Any] = {"query": current_query}
            if include_entities:
                payload["include"] = include_entities
            if exclude_entities:
                payload["exclude"] = exclude_entities

            response = await self.client.post(url, headers=self.port_headers, json=payload)
            response.raise_for_status()
            response_data: dict[str, Any] = response.json()

            # Yield the current page of entities
            yield response_data.get("entities", [])

            # Check if there's a next page
            next_cursor = response_data.get("next")
            if not next_cursor:
                break

            # Update the query with the next cursor
            current_query["from"] = next_cursor

    async def get_blueprint(self, blueprint_identifier: str) -> dict[str, Any]:
        await self.refresh_token_if_expired()
        url = f"{self.port_api_url}/blueprints/{blueprint_identifier}"
        response = await self.client.get(url, headers=self.port_headers)
        response.raise_for_status()
        response_data: dict[str, Any] = response.json()["blueprint"]
        return response_data
