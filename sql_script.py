from azure.storage.blob import BlobServiceClient
import pandas as pd
import sqlalchemy as sa

# --- CONFIGURATION ---
AZURE_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqWP...<rest of key>...==;"
    "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
)


container_name = "tesla-data"

# 1️⃣ Create BlobServiceClient instance first
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

# 2️⃣ Get the container client
container_client = blob_service_client.get_container_client(container_name)

# 3️⃣ Create the container if it doesn't exist
try:
    container_client.create_container()
except Exception:
    pass  # already exists

# MySQL Config
MYSQL_USER = "root"
MYSQL_PASSWORD = "RolexDaytona27"
MYSQL_HOST = "localhost"
MYSQL_DB = "tesla_db"

# --- CONNECT TO AZURITE ---
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)

def download_blob_to_df(blob_name):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    data = blob_client.download_blob().readall()
    return pd.read_csv(pd.compat.StringIO(data.decode("utf-8")))

# --- DOWNLOAD CSVs ---
stock_df = download_blob_to_df("tesla_stock_prices.csv")
financial_df = download_blob_to_df("tesla_financial_metrics.csv")

# --- CONNECT TO MYSQL ---
engine = sa.create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}")

# --- CREATE TABLES & LOAD DATA ---
stock_df.to_sql("tesla_stock_prices", con=engine, if_exists="replace", index=False)
financial_df.to_sql("tesla_financial_metrics", con=engine, if_exists="replace", index=False)

print("✅ Data loaded from AzURite to MySQL successfully!")
