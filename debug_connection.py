import os
import json
from azure.storage.blob import BlobServiceClient

# Load local.settings.json
with open('local.settings.json', 'r') as f:
    settings = json.load(f)
    
# Set environment variables
for key, value in settings.get('Values', {}).items():
    os.environ[key] = value

# Test the connection strings
print("=== Connection String Test ===")
azure_storage_conn = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
azure_webjobs_storage = os.environ.get('AzureWebJobsStorage')

print(f"AZURE_STORAGE_CONNECTION_STRING: {azure_storage_conn}")
print(f"AzureWebJobsStorage: {azure_webjobs_storage}")

# Test BlobServiceClient creation
try:
    client = BlobServiceClient.from_connection_string(azure_webjobs_storage)
    print(f"BlobServiceClient created successfully")
    print(f"Account name: {client.account_name}")
    print(f"Primary endpoint: {client.primary_endpoint}")
    
    # Test listing containers
    containers = list(client.list_containers())[:3]  # Just first 3
    print(f"Found {len(containers)} containers (showing first 3)")
    for container in containers:
        print(f"  - {container.name}")
        
except Exception as e:
    print(f"Error creating BlobServiceClient: {e}")
    print(f"Error type: {type(e)}")
