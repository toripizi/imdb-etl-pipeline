import os


def get_azure_base_path():
    """Returns the base Azure Data Lake path from environment variables."""
    storage_account = os.environ.get("AZURE_STORAGE_ACCOUNT")
    container = os.environ.get("AZURE_STORAGE_CONTAINER")
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net"
