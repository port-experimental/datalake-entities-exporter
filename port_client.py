import json
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple
import httpx
from loguru import logger
import os
from dotenv import load_dotenv

load_dotenv()

class PortClient:
    def __init__(self):
        self.port_client_id = os.getenv("PORT_CLIENT_ID")
        self.port_client_secret = os.getenv("PORT_CLIENT_SECRET")
        self.port_api_url = os.getenv("PORT_API_URL", "https://api.getport.io/v1")
        self.port_access_token = None
        self.token_expiry_time = datetime.now()
        self.port_headers = {}
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(60))

    async def get_access_token(self) -> Tuple[str, datetime]:
        credentials = {"clientId": self.port_client_id, "clientSecret": self.port_client_secret}
        token_response = await self.client.post(
            f"{self.port_api_url}/auth/access_token", json=credentials
        )
        token_response.raise_for_status()
        response_data = token_response.json()
        logger.debug(f"Access token response: {json.dumps(response_data, indent=2)}")
        access_token = response_data["accessToken"]
        expires_in = response_data["expiresIn"]
        token_expiry_time = datetime.now() + timedelta(seconds=expires_in)
        return access_token, token_expiry_time

    async def refresh_access_token(self) -> None:
        logger.info("Refreshing access token...")
        self.port_access_token, self.token_expiry_time = await self.get_access_token()
        self.port_headers = {"Authorization": f"Bearer {self.port_access_token}"}
        logger.info(f"New token received. Expiry time: {self.token_expiry_time}")

    async def refresh_token_if_expired(self) -> None:
        if datetime.now() >= self.token_expiry_time:
            await self.refresh_access_token()

    async def search_entities(self, blueprint_identifier: str, search_query: Dict[str, Any]) -> Dict[str, Any]:
        await self.refresh_token_if_expired()
        url = f"{self.port_api_url}/blueprints/{blueprint_identifier}/entities/search"
        logger.debug(f"Search query for {blueprint_identifier}: {json.dumps(search_query, indent=2)}")
        response = await self.client.post(url, headers=self.port_headers, json={
            "query": search_query
        })
        response.raise_for_status()
        response_data = response.json()
        logger.debug(f"Search response for {blueprint_identifier}: {json.dumps(response_data, indent=2)}")
        return response_data

    async def get_blueprint(self, blueprint_identifier: str) -> Dict[str, Any]:
        await self.refresh_token_if_expired()
        url = f"{self.port_api_url}/blueprints/{blueprint_identifier}"
        response = await self.client.get(url, headers=self.port_headers)
        response.raise_for_status()
        response_data = response.json()["blueprint"]
        logger.debug(f"Blueprint response for {blueprint_identifier}: {json.dumps(response_data, indent=2)}")
        return response_data