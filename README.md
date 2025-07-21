# PDF Classification Function - Storage Queue Trigger with CloudEvent Schema v1.0

## Overview

This Azure Function has been updated to process CloudEvent Schema v1.0 messages containing `Microsoft.Storage.BlobCreated` events via Storage Queue trigger. The function processes PDF files for classification and stores results with enhanced metadata including file_id and CloudEvent information.

## Key Features

### CloudEvent Schema v1.0 Support
- **Event Type**: `Microsoft.Storage.BlobCreated`
- **Schema Version**: CloudEvent v1.0
- **Message Source**: EventGrid to Storage Queue integration
- **Automatic Parsing**: Extracts blob information from CloudEvent structure

### Enhanced Metadata Tracking
1. **File ID Handling**: 
   - Priority 1: From blob metadata (`file_id` field)
   - Priority 2: CloudEvent ID
   - Priority 3: Auto-generated from timestamp and filename

2. **CloudEvent Information**: Preserved in outputs
   - CloudEvent ID, type, source, subject, time
   - Original blob data (contentType, contentLength, eTag, etc.)
   - Storage diagnostics information

3. **Classification Results**: Added to all metadata
   - Classification category
   - Confidence score
   - All classification result fields

## CloudEvent Message Structure

### Expected Input Format
```json
{
    "id": "b71ce573-701e-0062-7d30-f732a1062f49",
    "source": "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Storage/storageAccounts/xxx",
    "specversion": "1.0",
    "type": "Microsoft.Storage.BlobCreated",
    "subject": "/blobServices/default/containers/stampedstorage/blobs/Return Mail Scans 5.7.25_3.pdf",
    "time": "2025-07-17T15:37:08.3743318Z",
    "data": {
        "api": "PutBlob",
        "clientRequestId": "e607ab66-6323-11f0-b0be-6243a6c210f1",
        "requestId": "b71ce573-701e-0062-7d30-f732a1000000",
        "eTag": "0x8DDC547CA4AE764",
        "contentType": "application/pdf",
        "contentLength": 2992052,
        "blobType": "BlockBlob",
        "blobUrl": "https://account.blob.core.windows.net/stampedstorage/Return Mail Scans 5.7.25_3.pdf",
        "url": "https://account.blob.core.windows.net/stampedstorage/Return Mail Scans 5.7.25_3.pdf"
    }
}
```

## Configuration

### Environment Variables
```json
{
    "AzureWebJobsStorage": "your-storage-connection-string",
    "AZURE_STORAGE_CONNECTION_STRING": "your-storage-connection-string",
    "CLASSIFICATION_API_URL": "https://your-api-endpoint.com/api/pdf",
    "CLASSIFICATION_API_CODE": "your-api-code",
    "INPUT_CONTAINER": "stampedstorage",
    "CLASSIFICATION_CONTAINER": "classificationstorage",
    "RESULTS_CONTAINER": "resultstorage",
    "CLASSIFICATION_API_TIMEOUT": "300"
}
```

### Queue Setup
- **Queue Name**: `pdf-processing-queue`
- **Connection**: Uses `AzureWebJobsStorage` connection string
- **Message Format**: CloudEvent Schema v1.0
- **Event Source**: EventGrid subscription routing to Storage Queue

## Enhanced Output Metadata

### Classified PDF Metadata
- `file_id`: Unique file identifier
- `classification`: Classification category
- `confidence_score`: Confidence score (if available)
- `cloud_event_id`: Original CloudEvent ID
- `cloud_event_type`: CloudEvent type (Microsoft.Storage.BlobCreated)
- `cloud_event_time`: CloudEvent timestamp
- `cloud_event_source`: CloudEvent source
- `original_content_type`: Original blob content type
- `original_content_length`: Original blob content length
- `original_etag`: Original blob ETag
- `processed_at`: Processing timestamp
- `original_filename`: Original file name

### JSON Results Metadata
All classified PDF metadata plus:
- `result_*`: All classification result fields flattened
- `category`: Classification category (if different from classification)
- `blob_api`: Blob API used (e.g., PutBlob)

### JSON Content Structure
```json
{
    "original_filename": "Return Mail Scans 5.7.25_3.pdf",
    "processed_at": "2025-07-17T10:30:00.000Z",
    "file_id": "b71ce573-701e-0062-7d30-f732a1062f49",
    "classification_result": {
        "classification": "invoice",
        "confidence_score": 0.95,
        "category": "financial"
    },
    "original_blob_metadata": {
        "file_id": "custom-file-id-123"
    },
    "cloud_event": {
        "id": "b71ce573-701e-0062-7d30-f732a1062f49",
        "type": "Microsoft.Storage.BlobCreated",
        "source": "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Storage/storageAccounts/xxx",
        "subject": "/blobServices/default/containers/stampedstorage/blobs/Return Mail Scans 5.7.25_3.pdf",
        "time": "2025-07-17T15:37:08.3743318Z",
        "data": {
            "api": "PutBlob",
            "contentType": "application/pdf",
            "contentLength": 2992052,
            "blobUrl": "https://account.blob.core.windows.net/stampedstorage/Return Mail Scans 5.7.25_3.pdf"
        }
    },
    "metadata": {
        "version": "1.0",
        "source": "azure_function_classification",
        "file_id": "b71ce573-701e-0062-7d30-f732a1062f49"
    }
}
```

## Deployment and Setup

### EventGrid to Storage Queue Integration
1. **Create EventGrid Subscription**:
   - Source: Storage Account blob events
   - Event Types: `Microsoft.Storage.BlobCreated`
   - Endpoint: Storage Queue
   - Event Schema: CloudEvent Schema v1.0

2. **Queue Configuration**:
   - Queue Name: `pdf-processing-queue`
   - Connection: Use same storage account or different one
   - Message Format: CloudEvent v1.0

3. **Function Configuration**:
   - Trigger: Storage Queue
   - Queue Name: `pdf-processing-queue`
   - Connection: `AzureWebJobsStorage`

### Testing

#### Using the Helper Script
```bash
python queue_message_sender.py
```

#### Manual CloudEvent Creation
```json
{
    "id": "unique-event-id",
    "source": "/subscriptions/xxx/resourceGroups/xxx/providers/Microsoft.Storage/storageAccounts/xxx",
    "specversion": "1.0",
    "type": "Microsoft.Storage.BlobCreated",
    "subject": "/blobServices/default/containers/stampedstorage/blobs/yourfile.pdf",
    "time": "2025-07-17T15:37:08.3743318Z",
    "data": {
        "api": "PutBlob",
        "contentType": "application/pdf",
        "blobUrl": "https://account.blob.core.windows.net/stampedstorage/yourfile.pdf"
    }
}
```

### Monitoring and Debugging

#### Function Logs
- CloudEvent parsing details
- File ID resolution process
- Classification results
- Upload confirmations
- Error handling

#### Application Insights Queries
```kusto
traces 
| where message contains "CloudEvent" 
| order by timestamp desc
```

## Migration Benefits

### From EventGrid Direct to Storage Queue
1. **Reliability**: Built-in retry and dead letter queue support
2. **Scalability**: Better handling of high-volume processing
3. **Monitoring**: Enhanced visibility into message processing
4. **Flexibility**: Easier message routing and transformation

### CloudEvent Schema v1.0 Advantages
1. **Standardization**: Industry-standard event format
2. **Metadata Rich**: Comprehensive event context
3. **Interoperability**: Works with various event processors
4. **Extensibility**: Easy to add custom fields

## Error Handling

- **CloudEvent Parsing**: Validates required fields and structure
- **Blob Access**: Handles missing or inaccessible blobs
- **API Integration**: Graceful handling of classification API errors
- **Metadata Limits**: Truncates long values to fit Azure blob metadata limits
- **Queue Processing**: Prevents infinite retry loops

## Support

The function maintains full business logic compatibility while adding enhanced CloudEvent processing and comprehensive metadata tracking capabilities.
