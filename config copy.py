"""
Configuration file for PDF Classification Function
Contains all credentials and settings
"""
import os

class Config:
    # Azure Storage Settings
    AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    AZURE_WEBJOBS_STORAGE = os.environ.get('AzureWebJobsStorage', AZURE_STORAGE_CONNECTION_STRING)
    
    # Extract storage account name and key from connection string for explicit use
    STORAGE_ACCOUNT_NAME = os.environ.get('STORAGE_ACCOUNT_NAME', '')
    STORAGE_ACCOUNT_KEY = os.environ.get('STORAGE_ACCOUNT_KEY', '')
    
    # If not explicitly set, try to extract from connection string
    @classmethod
    def get_storage_account_name(cls):
        if cls.STORAGE_ACCOUNT_NAME:
            return cls.STORAGE_ACCOUNT_NAME
        
        if cls.AZURE_STORAGE_CONNECTION_STRING:
            try:
                # Clean the connection string to fix any Azure environment issues
                conn_str = cls.AZURE_STORAGE_CONNECTION_STRING.strip()
                
                # Fix potential URL corruption issue in Azure
                if '=core.windows.net' in conn_str:
                    conn_str = conn_str.replace('=core.windows.net', '.blob.core.windows.net')
                
                parts = conn_str.split(';')
                for part in parts:
                    part = part.strip()
                    if part.startswith('AccountName='):
                        account_name = part.split('=', 1)[1].strip()
                        return account_name
            except Exception as e:
                import logging
                logging.error(f"Error parsing account name from connection string: {str(e)}")
        return ''
    
    @classmethod
    def get_storage_account_key(cls):
        if cls.STORAGE_ACCOUNT_KEY:
            return cls.STORAGE_ACCOUNT_KEY
            
        if cls.AZURE_STORAGE_CONNECTION_STRING:
            try:
                # Clean the connection string to fix any Azure environment issues
                conn_str = cls.AZURE_STORAGE_CONNECTION_STRING.strip()
                
                # Fix potential URL corruption issue in Azure
                if '=core.windows.net' in conn_str:
                    conn_str = conn_str.replace('=core.windows.net', '.blob.core.windows.net')
                
                parts = conn_str.split(';')
                for part in parts:
                    part = part.strip()
                    if part.startswith('AccountKey='):
                        account_key = part.split('=', 1)[1].strip()
                        return account_key
            except Exception as e:
                import logging
                logging.error(f"Error parsing account key from connection string: {str(e)}")
        return ''
    
    # Container Settings
    INPUT_CONTAINER = os.environ.get('INPUT_CONTAINER', 'stampedstorage')
    CLASSIFICATION_CONTAINER = os.environ.get('CLASSIFICATION_CONTAINER', 'classificationstorage')
    RESULTS_CONTAINER = os.environ.get('RESULTS_CONTAINER', 'resultstorage')
    METADATA_CONTAINER = os.environ.get('METADATA_CONTAINER', 'jsonfiles')
    QUEUE_NAME = os.environ.get('QUEUE_NAME', 'classificationconsumerqa')
    
    # Classification API Settings
    CLASSIFICATION_API_URL = os.environ.get('CLASSIFICATION_API_URL')
    CLASSIFICATION_API_CODE = os.environ.get('CLASSIFICATION_API_CODE', '')
    CLASSIFICATION_API_TIMEOUT = int(os.environ.get('CLASSIFICATION_API_TIMEOUT', '300'))
    
    # Login Configuration (WMT API)
    LOGIN_URL = os.environ.get('LOGIN_URL', 'http://64.236.92.212/api/Session/login')
    LOGIN_EMAIL = os.environ.get('LOGIN_EMAIL', 'sheena.ailawadi@provana.com')
    LOGIN_PASSWORD = os.environ.get('LOGIN_PASSWORD', 'Welcome@123')
    TRANSACTION_URL = os.environ.get('TRANSACTION_URL', 'http://64.236.92.212/api/Transaction')
    
    # WMT API Settings
    PROCESS_CODE = os.environ.get('PROCESS_CODE', '715')
    ORG_ID = int(os.environ.get('ORG_ID', '4'))
    STATUS = int(os.environ.get('STATUS', '1'))  # Default status for WMT API records
    
    # Processing Settings
    MAX_TIMEOUT = 900.0  # Increased to 15 minutes for complex operations
    METADATA_MAX_LENGTH = 250
    
    # Network timeout settings for better reliability
    BLOB_OPERATION_TIMEOUT = 600  # 10 minutes for blob operations
    API_REQUEST_TIMEOUT = 60      # 1 minute for API requests
    WMT_API_TIMEOUT = 60          # 1 minute for WMT API calls
