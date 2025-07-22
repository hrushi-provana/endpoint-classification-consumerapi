# Azure Function URL Corruption Fix - Deployment Checklist

## Issue Identified
The error shows malformed URLs: `gurstelblobsftpqa.blob.core.windows.net=core.windows.net`
This indicates Azure environment variable corruption.

## Applied Fixes

### 1. Enhanced BlobServiceClient Creation (function_app.py)
- Added explicit URL construction method
- Added connection string corruption detection and auto-fix
- Enhanced error logging for debugging
- Multiple fallback methods

### 2. Improved Configuration Parsing (config.py)
- Added connection string cleaning methods
- Added URL corruption detection in config parsing
- Enhanced error handling

### 3. Dynamic Storage Account URL Construction
- Removed hardcoded storage account name from metadata URLs
- Made URLs dynamic based on actual account configuration

## Deployment Steps

### 1. Verify Azure Function App Settings
Check these environment variables in your Azure Function App Configuration:

```
AZURE_STORAGE_CONNECTION_STRING = DefaultEndpointsProtocol=https;AccountName=gurstelblobsftpqa;AccountKey=...;BlobEndpoint=https://gurstelblobsftpqa.blob.core.windows.net/;QueueEndpoint=https://gurstelblobsftpqa.queue.core.windows.net/;TableEndpoint=https://gurstelblobsftpqa.table.core.windows.net/;FileEndpoint=https://gurstelblobsftpqa.file.core.windows.net/

AzureWebJobsStorage = [Same as above]
```

### 2. Alternative: Use Explicit Account Settings
If connection string issues persist, add these explicit settings:

```
STORAGE_ACCOUNT_NAME = gurstelblobsftpqa
STORAGE_ACCOUNT_KEY = [Your storage account key]
```

### 3. Deploy and Monitor
1. Deploy the updated code
2. Monitor the Function App logs for these messages:
   - "Creating BlobServiceClient with explicit URL"
   - "Detected URL corruption in connection string, attempting fix"
   - Any error messages from get_blob_service_client()

### 4. Test Scenarios
The code now handles these scenarios:
- ✅ Normal connection string
- ✅ URL corruption (=core.windows.net issue)
- ✅ Missing connection string (fallback to explicit credentials)
- ✅ Dynamic storage account URL construction

## Expected Resolution
The enhanced error handling should:
1. Automatically detect and fix URL corruption
2. Use explicit URL construction as primary method
3. Provide detailed logging for troubleshooting
4. Fall back gracefully if one method fails

## Monitoring
Watch for these log messages to confirm the fix is working:
- "BlobServiceClient created with account: gurstelblobsftpqa"
- "Updated metadata JSON for file_id: [file_id]"

If you still see the original error pattern, check the Azure Function App Configuration for environment variable issues.
