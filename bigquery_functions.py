# Import Packages
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="sql-bigquery-341310-c5b9409e541c.json"

def list_dataset_tables(client, dataset, project="bigquery-public-data"):
    
    # Construct a Reference
    dataset_ref = client.dataset(dataset, project=project)
    
    # API Request - fetch Dataset
    dataset = client.get_dataset(dataset_ref)
    
    for table in list(client.list_tables(dataset)):
        print(table.table_id)

    return dataset_ref

def get_table(client, dataset_ref, table_id, preview=None):
    
    # Construct a Reference
    table_ref = dataset_ref.table(table_id)
    
    # API Request - fetch table
    table = client.get_table(table_ref)
    
    if preview!=None:
        print(client.list_rows(table, max_results=preview).to_dataframe())
        
    return table

def run_query(query, client, max_bytes):
    
    safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=max_bytes)
    query_job = client.query(query, job_config=safe_config)
    
    return query_job.to_dataframe()