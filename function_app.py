import logging
import json
import azure.functions as func
import aiohttp
import asyncio
import os
from datetime import datetime
from typing import Optional, Dict, Any
from azure.storage.blob import BlobServiceClient, ContentSettings

import requests

"""
PDF Classification Function - Storage Queue Trigger with CloudEvent Schema v1.0
================================================================================

This Azure Function processes PDF files for classification using a Storage Queue trigger.
The function processes CloudEvent Schema v1.0 messages containing Microsoft.Storage.BlobCreated events.

CloudEvent Message Format:
{
    "id": "unique-event-id",
    "source": "/subscriptions/.../storageAccounts/accountname",
    "specversion": "1.0",
    "type": "Microsoft.Storage.BlobCreated",
    "subject": "/blobServices/default/containers/containername/blobs/filename.pdf",
    "time": "2025-07-17T15:37:08.3743318Z",
    "data": {
        "api": "PutBlob",
        "contentType": "application/pdf",
        "contentLength": 2992052,
        "blobType": "BlockBlob",
        "blobUrl": "https://account.blob.core.windows.net/container/file.pdf",
        "url": "https://account.blob.core.windows.net/container/file.pdf",
        "eTag": "0x8DDC547CA4AE764",
        "clientRequestId": "client-request-id",
        "requestId": "request-id",
        "sequencer": "sequencer-value",
        "identity": "$superuser",
        "storageDiagnostics": {
            "batchId": "batch-id"
        }
    }
}

Key Features:
- Storage Queue trigger for reliable message processing
- CloudEvent Schema v1.0 parsing for Microsoft.Storage.BlobCreated events
- Retrieves file_id from blob metadata or generates from CloudEvent ID
- Adds classification results and confidence scores to blob metadata
- Stores comprehensive JSON results with CloudEvent and classification metadata
- Maintains all original business functionality

Required Environment Variables:
- AzureWebJobsStorage: Storage connection string for queue
- AZURE_STORAGE_CONNECTION_STRING: Blob storage connection string
- CLASSIFICATION_API_URL: Classification API endpoint
- CLASSIFICATION_API_CODE: API authentication code
- INPUT_CONTAINER: Source container for PDF files
- CLASSIFICATION_CONTAINER: Target container for classified PDFs
- RESULTS_CONTAINER: Container for JSON results

Workflow:
1. CloudEvent message triggers function via Storage Queue
2. Parse CloudEvent and extract blob information
3. Download PDF content and metadata from blob storage
4. Extract or generate file_id from blob metadata or CloudEvent ID
5. Call classification API with PDF content
6. Upload classified PDF with enhanced metadata including CloudEvent info
7. Save JSON results with comprehensive metadata including CloudEvent and classification details

File ID Resolution Priority:
1. file_id from blob metadata (if exists)
2. CloudEvent ID (if available)
3. Auto-generated from timestamp and filename
"""

AZURE_STORAGE_CONNECTION_STRING = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
AZURE_WEBJOBS_STORAGE = os.environ.get('AzureWebJobsStorage', AZURE_STORAGE_CONNECTION_STRING)
INPUT_CONTAINER = os.environ.get('INPUT_CONTAINER', 'stampedstorage')
CLASSIFICATION_CONTAINER = os.environ.get('CLASSIFICATION_CONTAINER', 'classificationstorage')
RESULTS_CONTAINER = os.environ.get('RESULTS_CONTAINER', 'resultstorage')
QUEUE_NAME = os.environ.get('QUEUE_NAME', 'claffificationconsumerqa')
CLASSIFICATION_API_URL = os.environ.get('CLASSIFICATION_API_URL')
CLASSIFICATION_API_CODE = os.environ.get('CLASSIFICATION_API_CODE')
CLASSIFICATION_API_TIMEOUT = int(os.environ.get('CLASSIFICATION_API_TIMEOUT', '300'))

def get_blob_service_client() -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(AZURE_WEBJOBS_STORAGE)

def sanitize_metadata_value(value: str) -> str:
    """
    Sanitize metadata values to comply with Azure Blob Storage requirements.
    Azure metadata values can only contain ASCII characters excluding control characters.
    """
    if not value:
        return ""
    
    # Convert to string and remove non-ASCII characters
    sanitized = str(value)
    
    # Remove or replace invalid characters
    # Azure allows: A-Z, a-z, 0-9, space, and basic punctuation
    # Remove control characters (ASCII 0-31 and 127)
    sanitized = ''.join(char for char in sanitized if ord(char) >= 32 and ord(char) < 127)
    
    # Replace problematic characters that might cause issues
    sanitized = sanitized.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    
    # Collapse multiple spaces and strip leading/trailing spaces
    sanitized = ' '.join(sanitized.split())
    
    # Truncate if too long (Azure metadata value limit is 8KB, but we keep it shorter)
    if len(sanitized) > 250:
        sanitized = sanitized[:247] + "..."
    
    return sanitized

app = func.FunctionApp()

@app.function_name(name="storageQueueTrigger")
@app.queue_trigger(arg_name="msg", queue_name="%QUEUE_NAME%", connection="AzureWebJobsStorage")
def storageQueueTrigger(msg: func.QueueMessage):
    """
    Storage Queue trigger function for processing PDF files
    Follows Azure Functions v2 programming model best practices
    Expected message format: CloudEvent Schema v1.0 with Microsoft.Storage.BlobCreated event
    """


    # Log the basic message information
    logging.info('Python Storage Queue trigger processed a message: %s', msg.get_body().decode('utf-8'))
    
    # Always log that function was triggered
    logging.info('🚀 Storage Queue function triggered!')
    
    try:
        # Parse CloudEvent message
        try:
            cloud_event = json.loads(msg.get_body().decode('utf-8'))
        except json.JSONDecodeError as e:
            logging.error(f'❌ Failed to parse CloudEvent JSON: {str(e)}')
            return
        
        # Validate CloudEvent structure
        if not cloud_event.get('type') or not cloud_event.get('data'):
            logging.error('❌ Invalid CloudEvent: Missing type or data')
            return
        
        # Check if this is a blob created event
        if cloud_event.get('type') != 'Microsoft.Storage.BlobCreated':
            logging.info(f'⏭️ Skipping non-blob-created event: {cloud_event.get("type")}')
            return
        
        # Extract event data
        event_data = cloud_event.get('data', {})
        subject = cloud_event.get('subject', '')
        
        # Parse blob information from CloudEvent
        blob_url = event_data.get('url', '') or event_data.get('blobUrl', '')
        container_name = ""
        blob_name = ""
        
        # Extract container and blob name from subject
        # Subject format: /blobServices/default/containers/containername/blobs/blobname
        if subject:
            try:
                subject_parts = subject.split('/')
                if 'containers' in subject_parts and 'blobs' in subject_parts:
                    container_idx = subject_parts.index('containers') + 1
                    blob_idx = subject_parts.index('blobs') + 1
                    if container_idx < len(subject_parts):
                        container_name = subject_parts[container_idx]
                    if blob_idx < len(subject_parts):
                        blob_name = '/'.join(subject_parts[blob_idx:])
                    
            except Exception as e:
                logging.error(f'❌ Error parsing subject: {str(e)}')
        
        # Fallback: Parse blob URL if subject parsing fails
        if not blob_name and blob_url:
            try:
                # Extract container and blob name from URL
                # URL format: https://accountname.blob.core.windows.net/container/blobname
                url_parts = blob_url.split('/')
                if len(url_parts) >= 5:
                    container_name = url_parts[3]  # Container name
                    blob_name = '/'.join(url_parts[4:])  # Blob name (can have folders)
                    
            except Exception as e:
                logging.error(f'❌ Error parsing blob URL: {str(e)}')
        
        # Validate required fields
        if not container_name or not blob_name:
            logging.error('❌ Could not extract container name or blob name from CloudEvent')
            return
        
        file_name = blob_name.split('/')[-1] if '/' in blob_name else blob_name
        logging.info(f'📄 Processing file: {file_name}')
        
        # Log CloudEvent key details
        logging.info(f'🔍 CloudEvent ID: {cloud_event.get("id")}')
        logging.info(f'🔍 CloudEvent type: {cloud_event.get("type")}')
        logging.info(f'🔍 Content type: {event_data.get("contentType")}')
        logging.info(f'🔍 Content length: {event_data.get("contentLength")}')
        
        # Check if the file is a PDF
        if not file_name.lower().endswith('.pdf'):
            logging.info(f'⏭️ Skipping non-PDF file: {file_name}')
            return
            
        # Check if it's already a classified file (avoid infinite loop)
        if '_classified' in file_name.lower():
            logging.info(f'⏭️ Skipping already classified file: {file_name}')
            return
        
        # Check if blob is in the correct container
        if container_name != INPUT_CONTAINER:
            logging.info(f'⏭️ Skipping file not in {INPUT_CONTAINER} container. Found in: {container_name}')
            return
            
        logging.info(f'✅ Processing PDF: {file_name} from container: {container_name}')
        
        # Download blob content and metadata
        pdf_content, blob_metadata = download_blob_content_with_metadata(container_name, blob_name)
        
        if not pdf_content:
            logging.error(f'❌ Failed to download blob: {blob_name}')
            return
        
        # Extract file_id from blob metadata or use CloudEvent ID
        file_id = ""
        if blob_metadata:
            file_id = blob_metadata.get('file_id', '')
        
        # If no file_id in metadata, use CloudEvent ID (no random generation)
        if not file_id:
            file_id = cloud_event.get('id', '')
            if file_id:
                logging.info(f'🆔 Using CloudEvent ID as file_id: {file_id}')
            else:
                logging.info('🆔 No file_id in metadata and no CloudEvent ID available')
        else:
            logging.info(f'🆔 File ID retrieved from blob metadata: {file_id}')
            
        # Run async processing
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            result = loop.run_until_complete(
                asyncio.wait_for(
                    process_pdf_classification(pdf_content, file_name, file_id, blob_metadata, cloud_event),
                    timeout=600.0
                )
            )
            
            if result['success']:
                logging.info(f'✅ Successfully processed: {file_name} -> {result.get("classification", "unknown")}')
                logging.info('🎉 Function execution completed successfully')
            else:
                logging.error(f'❌ Failed to process: {file_name} - {result.get("error", "unknown")}')
                raise Exception(f'Processing failed: {result.get("error", "unknown")}')
                
        except asyncio.TimeoutError:
            logging.error(f'⏱️ Processing timeout: {file_name}')
            raise Exception(f'Processing timeout: {file_name}')
        except Exception as e:
            logging.error(f'🔥 Processing error: {file_name} - {str(e)}')
            raise Exception(f'Processing error: {file_name} - {str(e)}')
        finally:
            try:
                loop.close()
            except:
                pass
            
    except Exception as e:
        logging.error(f'💥 CloudEvent processing error: {str(e)}')
        logging.error(f'💥 Message data: {msg.get_body().decode("utf-8") if hasattr(msg, "get_body") else "No message data"}')
        # Re-raise the exception to trigger message reprocessing
        raise e
        
    # Always log function completion
    logging.info('🏁 Function execution completed')

def download_blob_content_with_metadata(container_name: str, blob_name: str) -> tuple[Optional[bytes], Optional[Dict[str, str]]]:
    """Download blob content and metadata from storage"""
    try:
        blob_service_client = get_blob_service_client()
        
        logging.info(f'📥 Downloading blob: {blob_name}')
        
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name
        )
        
        # Check if blob exists
        if not blob_client.exists():
            logging.error(f'❌ Blob does not exist: {blob_name}')
            return None, None
        
        # Get blob properties first to retrieve metadata
        blob_properties = blob_client.get_blob_properties()
        blob_metadata = blob_properties.metadata if blob_properties.metadata else {}
        
        # Download blob content
        blob_data = blob_client.download_blob()
        content = blob_data.readall()
        
        logging.info(f'✅ Successfully downloaded blob: {blob_name} ({len(content)} bytes)')
        logging.info(f'📋 Blob metadata: {blob_metadata}')
        
        return content, blob_metadata
        
    except Exception as e:
        logging.error(f'❌ Error downloading blob {blob_name} from {container_name}: {str(e)}')
        return None, None

def download_blob_content(container_name: str, blob_name: str) -> Optional[bytes]:
    """Download blob content from storage (backward compatibility)"""
    content, _ = download_blob_content_with_metadata(container_name, blob_name)
    return content

async def process_pdf_classification(pdf_content: bytes, file_name: str, file_id: str, blob_metadata: Optional[Dict[str, str]] = None, cloud_event: Optional[Dict[str, Any]] = None) -> dict:
    """Process PDF through classification API and store results"""
    try:
        # Call classification API
        classification_result = await call_classification_api(pdf_content, file_name)
        if not classification_result:
            logging.error(f'❌ Classification API call failed for: {file_name}')
            return {'success': False, 'error': 'API call failed'}
        
        # Upload classified PDF with file_id
        upload_result = await upload_classified_pdf(pdf_content, file_name, classification_result, file_id, cloud_event)
        if not upload_result['success']:
            logging.error(f'❌ Upload failed for: {file_name} - {upload_result.get("error")}')
            return upload_result
        
        # Save JSON result with file_id and enhanced metadata
        json_result = await save_classification_json(file_name, classification_result, file_id, blob_metadata, cloud_event)
        if not json_result['success']:
            logging.error(f'❌ JSON save failed for: {file_name} - {json_result.get("error")}')
            return json_result
        
        return {
            'success': True,
            'classified_filename': upload_result['classified_filename'],
            'json_filename': json_result.get('json_filename'),
            'classification': classification_result.get('classification', 'unknown'),
            'confidence_score': classification_result.get('confidence_score', 0.0),
            'file_id': file_id,
            'cloud_event_id': cloud_event.get('id') if cloud_event else None,
            'json_stored': json_result['success']
        }
        
    except Exception as e:
        logging.error(f'❌ Error processing PDF: {str(e)}')
        return {'success': False, 'error': str(e)}

async def call_classification_api(pdf_content: bytes, file_name: str) -> Optional[Dict[str, Any]]:
    """Call the classification API"""
    try:
        # Use full URL if CLASSIFICATION_API_CODE is empty, otherwise construct URL
        if CLASSIFICATION_API_CODE:
            api_url = f"{CLASSIFICATION_API_URL}?code={CLASSIFICATION_API_CODE}"
        else:
            api_url = CLASSIFICATION_API_URL
        
        logging.info(f'🌐 Calling classification API for: {file_name}')
        
        # Prepare form data
        data = aiohttp.FormData()
        data.add_field('file', pdf_content, filename=file_name, content_type='application/pdf')
        
        timeout = aiohttp.ClientTimeout(total=CLASSIFICATION_API_TIMEOUT)
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(api_url, data=data) as response:
                logging.info(f'📊 API Response Status: {response.status}')
                
                if response.status == 200:
                    # Get response and parse JSON
                    raw_response = await response.text()
                    
                    # Parse JSON directly
                    try:
                        json_data = json.loads(raw_response)
                        if not json_data:
                            logging.error(f'Empty JSON response received')
                            return None
                            
                        result = await handle_classification_response_direct(json_data)
                        if result:
                            logging.info(f'✅ API call successful for: {file_name}')
                            logging.info(f'📋 Classification result: {result.get("classification", "unknown")}')
                            return result
                        else:
                            logging.error(f'Failed to process classification response for: {file_name}')
                            return None
                    except json.JSONDecodeError as e:
                        logging.error(f'Failed to parse JSON response: {str(e)}')
                        logging.error(f'Raw response: {raw_response[:200]}')
                        return None
                else:
                    response_text = await response.text()
                    logging.error(f'❌ API error {response.status}: {response_text}')
                    return None
                    
    except asyncio.TimeoutError:
        logging.error(f'⏱️ API timeout for: {file_name}')
        return None
    except Exception as e:
        logging.error(f'❌ API call error for {file_name}: {str(e)}')
        return None

async def handle_classification_response_direct(json_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Handle API response JSON data directly - supports both old and new API response formats"""
    try:
        # Handle new detailed API response format (structure: {"json": [{"File Name": "...", etc.}]})
        if 'json' in json_data and isinstance(json_data['json'], list) and len(json_data['json']) > 0:
            # Extract classification data from the new format
            classification_data = json_data['json'][0]
            
            # Create structured response with all the new fields
            structured_result = {
                'classification': classification_data.get('Predicted Category', 'unknown'),
                'subcategory': classification_data.get('Predicted Subcategory', 'N/A'),
                'expected_category': classification_data.get('Expected Category', 'N/A'),
                'expected_subcategory': classification_data.get('Expected Subcategory', 'N/A'),
                'evaluation_type': classification_data.get('Evaluation Type', 'N/A'),
                'evaluation_verdict': classification_data.get('Evaluation Verdict', 'N/A'),
                'evaluation_notes': classification_data.get('Evaluation Notes', 'N/A'),
                'classifier_reasoning': classification_data.get('Classifier Reasoning', 'N/A'),
                'classifier_keywords': classification_data.get('Classifier Keywords', 'N/A'),
                'file_name': classification_data.get('File Name', 'N/A'),
                'confidence_score': 'N/A',  # Not available in new format
                # Include the full original response for completeness
                'full_response': json_data
            }
            
            return structured_result
        
        # Handle old detailed API response format (structure: {"classification_result": {"json": [{"File Name": "...", etc.}]}})
        elif 'classification_result' in json_data and 'json' in json_data['classification_result']:
            # Extract classification data from the old detailed format  
            classification_data = json_data['classification_result']['json'][0] if json_data['classification_result']['json'] else {}
            
            # Create structured response with all the new fields
            structured_result = {
                'classification': classification_data.get('Predicted Category', 'unknown'),
                'subcategory': classification_data.get('Predicted Subcategory', 'N/A'),
                'expected_category': classification_data.get('Expected Category', 'N/A'),
                'expected_subcategory': classification_data.get('Expected Subcategory', 'N/A'),
                'evaluation_type': classification_data.get('Evaluation Type', 'N/A'),
                'evaluation_verdict': classification_data.get('Evaluation Verdict', 'N/A'),
                'evaluation_notes': classification_data.get('Evaluation Notes', 'N/A'),
                'classifier_reasoning': classification_data.get('Classifier Reasoning', 'N/A'),
                'classifier_keywords': classification_data.get('Classifier Keywords', 'N/A'),
                'file_name': classification_data.get('File Name', 'N/A'),
                'confidence_score': 'N/A',  # Not available in new format
                # Include the full original response for completeness
                'full_response': json_data
            }
            
            return structured_result
        
        # Handle old simple response format (backward compatibility)
        elif 'classification' in json_data:
            return json_data
            
        # If structure doesn't match any format, return as-is
        else:
            logging.warning(f'🔍 Unknown API response format: {json_data}')
            return json_data
                
    except Exception as e:
        logging.error(f'Response handling error: {str(e)}')
        return None

async def upload_classified_pdf(pdf_content: bytes, original_file_name: str, classification_result: Dict[str, Any], file_id: str = "", cloud_event: Optional[Dict[str, Any]] = None) -> dict:
    """Upload PDF to classification storage with comprehensive metadata"""
    try:
        name_without_ext = os.path.splitext(original_file_name)[0]
        extension = os.path.splitext(original_file_name)[1]
        classification = classification_result.get('classification', 'unknown')
        classified_filename = f"{name_without_ext}_classified_{classification}{extension}"
        
        logging.info(f'📤 Uploading classified PDF: {classified_filename}')
        
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=CLASSIFICATION_CONTAINER,
            blob=classified_filename
        )
        
        # Prepare metadata with all classification details
        metadata = {
            'classification': sanitize_metadata_value(str(classification)),
            'processed_at': sanitize_metadata_value(datetime.utcnow().isoformat()),
            'original_filename': sanitize_metadata_value(original_file_name)
        }
        
        # Add file_id only if it exists (not empty)
        if file_id:
            metadata['file_id'] = sanitize_metadata_value(str(file_id))
        
        # Add all classification result fields to metadata
        classification_fields = {
            'subcategory': classification_result.get('subcategory', 'N/A'),
            'expected_category': classification_result.get('expected_category', 'N/A'),
            'expected_subcategory': classification_result.get('expected_subcategory', 'N/A'),
            'evaluation_type': classification_result.get('evaluation_type', 'N/A'),
            'evaluation_verdict': classification_result.get('evaluation_verdict', 'N/A'),
            'classifier_reasoning': classification_result.get('classifier_reasoning', 'N/A'),
            'classifier_keywords': classification_result.get('classifier_keywords', 'N/A'),
            'confidence_score': classification_result.get('confidence_score', 'N/A')
        }
        
        # Add classification fields to metadata (sanitize values)
        for key, value in classification_fields.items():
            metadata[key] = sanitize_metadata_value(str(value))
        
        # Add CloudEvent information if available
        if cloud_event:
            metadata['cloud_event_id'] = sanitize_metadata_value(str(cloud_event.get('id', '')))
            metadata['cloud_event_type'] = sanitize_metadata_value(str(cloud_event.get('type', '')))
            metadata['cloud_event_time'] = sanitize_metadata_value(str(cloud_event.get('time', '')))
            metadata['cloud_event_source'] = sanitize_metadata_value(str(cloud_event.get('source', '')))
            
            # Add event data information
            event_data = cloud_event.get('data', {})
            if event_data.get('contentType'):
                metadata['original_content_type'] = sanitize_metadata_value(str(event_data.get('contentType')))
            if event_data.get('contentLength'):
                metadata['original_content_length'] = sanitize_metadata_value(str(event_data.get('contentLength')))
            if event_data.get('eTag'):
                metadata['original_etag'] = sanitize_metadata_value(str(event_data.get('eTag')))
            if event_data.get('api'):
                metadata['blob_api'] = sanitize_metadata_value(str(event_data.get('api')))
        
        # Upload with comprehensive metadata
        blob_client.upload_blob(
            data=pdf_content,
            content_settings=ContentSettings(content_type='application/pdf'),
            metadata=metadata,
            overwrite=True
        )
        
        logging.info(f'✅ Successfully uploaded classified PDF: {classified_filename} ({len(pdf_content)} bytes)')
        if file_id:
            logging.info(f'🆔 File ID included in metadata: {file_id}')
        
        return {
            'success': True,
            'classified_filename': classified_filename,
            'file_size': len(pdf_content),
            'file_id': file_id,
            'cloud_event_id': cloud_event.get('id') if cloud_event else None
        }
        
    except Exception as e:
        logging.error(f'❌ Upload error: {str(e)}')
        return {'success': False, 'error': str(e)}

async def save_classification_json(original_file_name: str, classification_result: Dict[str, Any], file_id: str = "", blob_metadata: Optional[Dict[str, str]] = None, cloud_event: Optional[Dict[str, Any]] = None) -> dict:
    """Save JSON result with enhanced metadata including all classification details"""
    try:
        name_without_ext = os.path.splitext(original_file_name)[0]
        json_filename = f"{name_without_ext}_classification_result.json"
        
        logging.info(f'💾 Saving classification JSON: {json_filename}')
        
        # Create JSON content with enhanced metadata
        json_data = {
            'original_filename': original_file_name,
            'processed_at': datetime.utcnow().isoformat(),
            'classification_result': classification_result,
            'metadata': {
                'version': '1.0',
                'source': 'azure_function_classification'
            }
        }
        
        # Add file_id only if it exists (not empty)
        if file_id:
            json_data['file_id'] = file_id
            json_data['metadata']['file_id'] = file_id
        
        # Add original blob metadata if available
        if blob_metadata:
            json_data['original_blob_metadata'] = blob_metadata
        
        # Add CloudEvent information if available
        if cloud_event:
            json_data['cloud_event'] = {
                'id': cloud_event.get('id'),
                'type': cloud_event.get('type'),
                'source': cloud_event.get('source'),
                'subject': cloud_event.get('subject'),
                'time': cloud_event.get('time'),
                'specversion': cloud_event.get('specversion'),
                'data': cloud_event.get('data')
            }
        
        json_content = json.dumps(json_data, indent=2, ensure_ascii=False)
        json_bytes = json_content.encode('utf-8')
        
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=RESULTS_CONTAINER,
            blob=json_filename
        )
        
        # Prepare comprehensive metadata for the JSON blob
        blob_metadata_dict = {
            'content_type': 'application/json',
            'original_filename': sanitize_metadata_value(original_file_name),
            'processed_at': sanitize_metadata_value(datetime.utcnow().isoformat()),
            'classification': sanitize_metadata_value(str(classification_result.get('classification', 'unknown'))),
        }
        
        # Add file_id to metadata only if it exists (not empty)
        if file_id:
            blob_metadata_dict['file_id'] = sanitize_metadata_value(str(file_id))
        
        # Add all classification result fields to metadata
        classification_fields = {
            'subcategory': classification_result.get('subcategory', 'N/A'),
            'expected_category': classification_result.get('expected_category', 'N/A'),
            'expected_subcategory': classification_result.get('expected_subcategory', 'N/A'),
            'evaluation_type': classification_result.get('evaluation_type', 'N/A'),
            'evaluation_verdict': classification_result.get('evaluation_verdict', 'N/A'),
            'classifier_reasoning': classification_result.get('classifier_reasoning', 'N/A'),
            'classifier_keywords': classification_result.get('classifier_keywords', 'N/A'),
            'confidence_score': classification_result.get('confidence_score', 'N/A')
        }
        
        # Add classification fields to metadata (sanitize values)
        for key, value in classification_fields.items():
            blob_metadata_dict[key] = sanitize_metadata_value(str(value))
        
        # Add CloudEvent information to metadata if available
        if cloud_event:
            blob_metadata_dict['cloud_event_id'] = sanitize_metadata_value(str(cloud_event.get('id', '')))
            blob_metadata_dict['cloud_event_type'] = sanitize_metadata_value(str(cloud_event.get('type', '')))
            blob_metadata_dict['cloud_event_time'] = sanitize_metadata_value(str(cloud_event.get('time', '')))
            blob_metadata_dict['cloud_event_source'] = sanitize_metadata_value(str(cloud_event.get('source', '')))
            
            # Add event data information
            event_data = cloud_event.get('data', {})
            if event_data.get('contentType'):
                blob_metadata_dict['original_content_type'] = sanitize_metadata_value(str(event_data.get('contentType')))
            if event_data.get('contentLength'):
                blob_metadata_dict['original_content_length'] = sanitize_metadata_value(str(event_data.get('contentLength')))
            if event_data.get('eTag'):
                blob_metadata_dict['original_etag'] = sanitize_metadata_value(str(event_data.get('eTag')))
            if event_data.get('api'):
                blob_metadata_dict['blob_api'] = sanitize_metadata_value(str(event_data.get('api')))
        
        # Add evaluation notes to metadata (sanitized)
        if 'evaluation_notes' in classification_result:
            blob_metadata_dict['evaluation_notes'] = sanitize_metadata_value(str(classification_result['evaluation_notes']))
        
        blob_client.upload_blob(
            data=json_bytes,
            content_settings=ContentSettings(content_type='application/json'),
            metadata=blob_metadata_dict,
            overwrite=True
        )
        
        logging.info(f'✅ Successfully saved JSON result: {json_filename} ({len(json_bytes)} bytes)')
        if file_id:
            logging.info(f'🆔 File ID included in JSON and metadata: {file_id}')
        logging.info(f'📋 Classification: {classification_result.get("classification", "unknown")}')
        
        return {
            'success': True,
            'json_filename': json_filename,
            'file_size': len(json_bytes),
            'file_id': file_id,
            'cloud_event_id': cloud_event.get('id') if cloud_event else None,
            'classification': classification_result.get('classification', 'unknown'),
            'confidence_score': classification_result.get('confidence_score', 'N/A')
        }
        
    except Exception as e:
        logging.error(f'❌ JSON save error: {str(e)}')
        return {'success': False, 'error': str(e)}

