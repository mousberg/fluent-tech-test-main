from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from tabulate import tabulate
import json
from dotenv import load_dotenv
import os

load_dotenv()

# Dummy service account JSON - Replace this with your actual service account JSON content
SERVICE_ACCOUNT_JSON = os.getenv("SERVICE_ACCOUNT_JSON")

# BigQuery Default Dataset - Replace this with your actual dataset name
DEFAULT_DATASET = os.getenv("DEFAULT_DATASET")

# Limit the number of rows to fetch
MAX_RESULTS = os.getenv("MAX_RESULTS") or 10

if SERVICE_ACCOUNT_JSON is None:
    raise ValueError("SERVICE_ACCOUNT_JSON is not set in the environment variables.")

if DEFAULT_DATASET is None:
    raise ValueError("DEFAULT_DATASET is not set in the environment variables.")

def format_value(value):
    """Format values for better readability"""
    if isinstance(value, float):
        if value > 1000000:
            return f"${value/1000000:.2f}M"  # Divide by 1M first
        return f"${value:,.2f}"
    if isinstance(value, str) and '+00:00' in value:
        return value.split()[0]  # Keep just the date part
    return value

def query_bigquery(sql: str) -> None:
    """Execute a BigQuery SQL query and print results"""
    client = bigquery.Client.from_service_account_info(
        json.loads(os.getenv('SERVICE_ACCOUNT_JSON'))
    )
    
    query_job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            default_dataset=os.getenv('DEFAULT_DATASET')
        )
    )
    
    results = query_job.result()
    rows = list(results)
    
    print(f"Total Rows: {len(rows)}")
    if rows:
        print("First 10 Rows:")
        formatted_rows = [
            [format_value(value) for value in row.values()]
            for row in rows[:10]
        ]
        print(tabulate(
            formatted_rows,
            headers=list(rows[0].keys()),
            tablefmt="grid"
        ))

# Example Usage
if __name__ == "__main__":
    sample_query = """
    SELECT * 
    FROM orders
    """
    query_bigquery(sample_query)
