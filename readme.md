# IMDB ETL Pipeline

## How to Run

1. **Build the Docker image**:
   ```
   make build
   ```

2. **Create environment file**:
   Create a `.env` file based on the provided `.env.example`:
   ```
   cp .env.example .env
   ```
   Update the values with your Azure Storage credentials:
   ```
   AZURE_STORAGE_ACCOUNT="your-storage-account"
   AZURE_STORAGE_CONTAINER="your-container"
   AZURE_ACCOUNT_KEY="your-account-key"
   ```

3. **Start the application**:
   ```
   make start
   ```
   Visit http://localhost:8001/ and login with airflow:airflow

## What the App Does

This is an ETL (Extract, Transform, Load) pipeline that processes IMDB movie data. The pipeline:

1. Extracts data from IMDB using a scraper component
2. Transforms the data using Apache Spark
3. Loads the processed data to Azure Data Lake

The pipeline processes movie information including cast, directors, ratings, genres, and other metadata, structuring it for recommendation purposes.

## Architecture

- **Apache Airflow**: Orchestrates the ETL workflow
- **Apache Spark**: Handles data processing and transformation
- **Azure Data Lake**: Destination for processed data
- **Docker**: Containerizes the entire application for easy deployment

The pipeline consists of two main tasks:
1. `ImdbScraperOperator`: Extracts data from IMDB
2. `ImdbOperator`: Processes the data with Spark and loads it to Azure

## Important Note on IMDB Data Usage

The `info.txt` file contains information from the official IMDB website regarding data usage policies. Please note that scraping IMDB data is not permitted according to their terms of service. 

However, IMDB provides free datasets for non-commercial use through their IMDb Datasets service. These datasets are updated daily and contain various information about titles, ratings, and more. For production use, it is recommended to use these official datasets instead of scraping.

You can find more information about the official IMDB datasets at: https://developer.imdb.com/non-commercial-datasets/
