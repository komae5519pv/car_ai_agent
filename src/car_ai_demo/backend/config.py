"""Configuration module for Databricks and application settings."""

import os
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Databricks Settings
    databricks_host: str = ""
    databricks_token: Optional[str] = None
    databricks_client_id: Optional[str] = None
    databricks_client_secret: Optional[str] = None
    databricks_warehouse_id: str = ""
    databricks_profile: str = "DEFAULT"

    # Unity Catalog Settings
    catalog: str = "konomi_demo_catalog"
    schema_name: str = "car_ai_demo"

    # Agent Endpoint (Multi-Agent Supervisor)
    agent_endpoint_name: str = ""

    # Genie Space IDs
    sales_mypage_genie_space_id: str = "01f13bd256c11b0cb17f818f4bd28b71"

    # Foundation Model API Settings (推薦・インサイト生成用)
    llm_model: str = "databricks-claude-sonnet-4"
    llm_max_tokens: int = 4096

    # App Settings
    app_name: str = "car-ai-demo"
    debug: bool = False
    port: int = 8000

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


def is_databricks_app() -> bool:
    """Check if running inside Databricks Apps environment."""
    return bool(os.environ.get("DATABRICKS_APP_NAME"))


def get_databricks_host() -> str:
    """Get Databricks host URL with https:// prefix."""
    settings = get_settings()
    host = settings.databricks_host or os.environ.get("DATABRICKS_HOST", "")

    # In Databricks Apps, DATABRICKS_HOST is just hostname without scheme
    if host and not host.startswith("http"):
        host = f"https://{host}"

    return host


def get_oauth_token() -> Optional[str]:
    """Get OAuth token for Databricks authentication."""
    settings = get_settings()

    # First try explicit token
    if settings.databricks_token:
        return settings.databricks_token

    # Try service principal OAuth
    if is_databricks_app():
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()

            # Use authenticate() for OAuth/U2M auth
            if w.config.token:
                return w.config.token

            auth_headers = w.config.authenticate()
            if auth_headers and "Authorization" in auth_headers:
                return auth_headers["Authorization"].replace("Bearer ", "")
        except Exception as e:
            print(f"Failed to get OAuth token: {e}")

    return None


def get_full_table_name(table_name: str) -> str:
    """Get fully qualified table name with catalog and schema."""
    settings = get_settings()
    return f"{settings.catalog}.{settings.schema_name}.{table_name}"
