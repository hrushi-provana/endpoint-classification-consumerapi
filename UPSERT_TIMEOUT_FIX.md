# Database Upsert Logic & Timeout Fix - Implementation Summary

## Issues Addressed

### 1. Database Upsert Logic
**Problem**: Need to check if file_id exists in database, then update if exists or create if not.

**Solution**: Implemented comprehensive upsert logic in `auth.py`:

#### New Methods Added:
- `check_record_exists(file_id)` - Checks if record exists via GET request
- `create_wmt_record(payload)` - Creates new record via POST request  
- `update_wmt_record(file_id, payload)` - Updates existing record via PUT request
- Enhanced `send_to_wmt_api()` - Orchestrates the upsert logic

#### Workflow:
1. **Check**: GET `/api/Transaction/{file_id}` 
   - Returns 200 if exists, 404 if not found
2. **Update**: If exists, PUT `/api/Transaction/{file_id}` with payload
3. **Create**: If not exists, POST `/api/Transaction` with payload

### 2. Timeout Error Fix
**Problem**: `{"name": "RestError", "code": "ETIMEOUT"}` errors during blob operations and API calls.

**Solution**: Enhanced timeout configurations across all components:

#### Enhanced Timeouts:
- **Azure Blob Storage**: 
  - Connection timeout: 5 minutes (300s)
  - Read timeout: 10 minutes (600s)
- **Classification API**: Enhanced aiohttp.ClientTimeout with granular settings
- **WMT API**: 60 seconds for all operations
- **Overall Processing**: 15 minutes (900s) for complex operations

#### Configuration Updates (`config.py`):
```python
MAX_TIMEOUT = 900.0  # 15 minutes for complex operations
BLOB_OPERATION_TIMEOUT = 600  # 10 minutes for blob operations
API_REQUEST_TIMEOUT = 60      # 1 minute for API requests
WMT_API_TIMEOUT = 60          # 1 minute for WMT API calls
```

## Code Changes Summary

### auth.py - Enhanced WMT API Operations
```python
# NEW: Upsert logic with proper error handling
def send_to_wmt_api(self, wmt_payload: dict) -> bool:
    # 1. Check if record exists
    # 2. Update if exists, create if not
    # 3. Comprehensive error handling and logging

def check_record_exists(self, file_id: str) -> bool:
    # GET request to check existence
    
def create_wmt_record(self, wmt_payload: dict) -> bool:
    # POST request to create new record
    
def update_wmt_record(self, file_id: str, wmt_payload: dict) -> bool:
    # PUT request to update existing record
```

### function_app.py - Enhanced Blob Service & Timeouts
```python
# Enhanced BlobServiceClient with timeout configuration
def get_blob_service_client() -> BlobServiceClient:
    # Added connection_timeout=300, read_timeout=600
    
# Enhanced aiohttp timeout configuration
timeout = aiohttp.ClientTimeout(
    total=Config.CLASSIFICATION_API_TIMEOUT,
    connect=60,     # Connection timeout
    sock_read=300,  # Socket read timeout  
    sock_connect=30 # Socket connect timeout
)

# Enhanced asyncio processing with 15-minute timeout
result = loop.run_until_complete(
    asyncio.wait_for(
        process_pdf_classification(...),
        timeout=900.0  # 15 minutes
    )
)
```

### config.py - Centralized Timeout Configuration
```python
# New timeout settings for better reliability
BLOB_OPERATION_TIMEOUT = 600  # 10 minutes
API_REQUEST_TIMEOUT = 60      # 1 minute  
WMT_API_TIMEOUT = 60          # 1 minute
MAX_TIMEOUT = 900.0           # 15 minutes
```

## Expected Behavior

### Database Operations:
1. **New File**: Creates new record in WMT database
2. **Existing File**: Updates existing record with new classification data
3. **Error Handling**: Graceful fallback and detailed logging

### Timeout Resolution:
1. **Blob Operations**: Can handle large files up to 10 minutes
2. **API Calls**: Robust timeout handling with retry capabilities
3. **Overall Processing**: 15-minute total timeout for complex workflows

## Testing Scenarios

✅ **New File Processing**:
- Check if file_id exists in database (404 response)
- Create new record via POST
- Populate all classification fields

✅ **Existing File Update**:
- Check if file_id exists in database (200 response)  
- Update existing record via PUT
- Merge new classification data with existing fields

✅ **Timeout Handling**:
- Large file processing (>100MB)
- Slow network conditions
- API rate limiting scenarios

## Monitoring & Logs

Look for these log messages to confirm proper operation:

### Database Operations:
- "Record exists for file_id {file_id}, updating..."
- "No record found for file_id {file_id}, creating new..."
- "Successfully created new record in WMT Transaction API"
- "Successfully updated record for file_id: {file_id}"

### Timeout Handling:
- "Creating BlobServiceClient with explicit URL: ..."
- "Processing timeout after 15 minutes: {file_name}"
- Any timeout-related error messages with context

The system now provides robust database upsert logic and comprehensive timeout handling for all network operations.
