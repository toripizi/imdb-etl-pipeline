import os
import logging


def get_azure_base_path():
    """Returns the base Azure Data Lake path from environment variables."""
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT")
    container = os.environ.get("AZURE_STORAGE_CONTAINER")

    if not storage_account:
        raise ValueError("AZURE_STORAGE_ACCOUNT environment variable is not set")
        
    if not container:
        raise ValueError("AZURE_STORAGE_CONTAINER environment variable is not set")
    
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net"


def get_azure_path(ds: str):
    base_path = get_azure_base_path()
    return f"{base_path}/{ds}/imdb_processed.json"
