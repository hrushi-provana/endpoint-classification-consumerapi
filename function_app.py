import logging
import json
import azure.functions as func
import aiohttp
import asyncio
import os
from datetime import datetime
from typing import Optional, Dict, Any
from azure.storage.blob import BlobServiceClient, ContentSettings

# Import our modules
from config import Config
from auth import AuthHelper

app = func.FunctionApp()

def extract_confidence_score(evaluation_verdict: str) -> int:
    """Extract numeric confidence score from evaluation_verdict (e.g., '5/10' -> 5)"""
    try:
        if not evaluation_verdict or evaluation_verdict == 'N/A':
            return 0
        
        # Handle formats like "5/10", "7/10", etc.
        if '/' in evaluation_verdict:
            numerator = evaluation_verdict.split('/')[0].strip()
            return int(numerator)
        
        # Handle pure numeric strings
        if evaluation_verdict.isdigit():
            return int(evaluation_verdict)
        
        # Default case
        return 0
        
    except (ValueError, IndexError, AttributeError):
        logging.warning(f"Could not parse confidence score from: {evaluation_verdict}")
        return 0

def get_blob_service_client() -> BlobServiceClient:
    """Create BlobServiceClient using connection string with enhanced error handling"""
    try:
        # Method 1: Extract components and build explicitly (most reliable)
        account_name = Config.get_storage_account_name()
        account_key = Config.get_storage_account_key()
        
        if account_name and account_key:
            # Explicitly construct account URL to avoid parsing issues
            account_url = f"https://{account_name}.blob.core.windows.net"
            logging.info(f"Creating BlobServiceClient with explicit URL: {account_url}")
            
            # Create with custom timeout configuration to handle network issues
            blob_client = BlobServiceClient(
                account_url=account_url, 
                credential=account_key,
                connection_timeout=300,  # 5 minutes connection timeout
                read_timeout=600        # 10 minutes read timeout
            )
            return blob_client
        
        # Method 2: Fallback to connection string method with URL fix
        if Config.AZURE_STORAGE_CONNECTION_STRING:
            logging.info("Falling back to connection string method")
            
            # Fix potential Azure environment URL corruption
            conn_str = Config.AZURE_STORAGE_CONNECTION_STRING
            if '=core.windows.net' in conn_str:
                logging.warning("Detected URL corruption in connection string, attempting fix...")
                conn_str = conn_str.replace('=core.windows.net', '.blob.core.windows.net')
            
            blob_client = BlobServiceClient.from_connection_string(
                conn_str,
                connection_timeout=300,  # 5 minutes connection timeout
                read_timeout=600        # 10 minutes read timeout
            )
            logging.info(f"BlobServiceClient created with account: {blob_client.account_name}")
            return blob_client
        
        raise ValueError("No valid storage credentials found")
            
    except Exception as e:
        logging.error(f"Error creating BlobServiceClient: {str(e)}")
        logging.error(f"Connection string available: {bool(Config.AZURE_STORAGE_CONNECTION_STRING)}")
        logging.error(f"Account name: {Config.get_storage_account_name()}")
        logging.error(f"Account key available: {bool(Config.get_storage_account_key())}")
        
        # Last resort: Try to create a minimal client
        try:
            logging.info("Attempting last resort connection...")
            fallback_conn_str = Config.AZURE_STORAGE_CONNECTION_STRING or Config.AZURE_WEBJOBS_STORAGE
            if fallback_conn_str and '=core.windows.net' in fallback_conn_str:
                fallback_conn_str = fallback_conn_str.replace('=core.windows.net', '.blob.core.windows.net')
            return BlobServiceClient.from_connection_string(
                fallback_conn_str,
                connection_timeout=300,  # 5 minutes connection timeout
                read_timeout=600        # 10 minutes read timeout
            )
        except Exception as final_error:
            logging.error(f"Final fallback failed: {str(final_error)}")
            raise e

def sanitize_metadata_value(value: str) -> str:
    """Sanitize metadata values for Azure Blob Storage requirements"""
    if not value:
        return ""
    
    sanitized = str(value)
    sanitized = ''.join(char for char in sanitized if ord(char) >= 32 and ord(char) < 127)
    sanitized = sanitized.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    sanitized = ' '.join(sanitized.split())
    
    if len(sanitized) > 250:
        sanitized = sanitized[:247] + "..."
    
    return sanitized

@app.queue_trigger(arg_name="msg", queue_name="%QUEUE_NAME%", connection="AzureWebJobsStorage")
def storageQueueTrigger(msg: func.QueueMessage):
    """Storage Queue trigger function for processing PDF files"""
    
    logging.info('Processing message: %s', msg.get_body().decode('utf-8'))
    
    try:
        # Parse CloudEvent message
        cloud_event = json.loads(msg.get_body().decode('utf-8'))
        
        # Validate CloudEvent structure
        if not cloud_event.get('type') or not cloud_event.get('data'):
            logging.error('Invalid CloudEvent: Missing type or data')
            return
        
        # Check if this is a blob created event
        if cloud_event.get('type') != 'Microsoft.Storage.BlobCreated':
            logging.info(f'Skipping non-blob-created event: {cloud_event.get("type")}')
            return
        
        # Extract event data
        event_data = cloud_event.get('data', {})
        subject = cloud_event.get('subject', '')
        
        # Parse blob information from CloudEvent
        blob_url = event_data.get('url', '') or event_data.get('blobUrl', '')
        container_name = ""
        blob_name = ""
        
        # Extract container and blob name from subject
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
                logging.error(f'Error parsing subject: {str(e)}')
        
        # Fallback: Parse blob URL if subject parsing fails
        if not blob_name and blob_url:
            try:
                url_parts = blob_url.split('/')
                if len(url_parts) >= 5:
                    container_name = url_parts[3]
                    blob_name = '/'.join(url_parts[4:])
            except Exception as e:
                logging.error(f'Error parsing blob URL: {str(e)}')
        
        # Validate required fields
        if not container_name or not blob_name:
            logging.error('Could not extract container name or blob name from CloudEvent')
            return
        
        file_name = blob_name.split('/')[-1] if '/' in blob_name else blob_name
        
        # Check if the file is a PDF
        if not file_name.lower().endswith('.pdf'):
            logging.info(f'Skipping non-PDF file: {file_name}')
            return
            
        # Check if it's already a classified file (avoid infinite loop)
        if '_classified' in file_name.lower():
            logging.info(f'Skipping already classified file: {file_name}')
            return
        
        # Check if blob is in the correct container
        if container_name != Config.INPUT_CONTAINER:
            logging.info(f'Skipping file not in {Config.INPUT_CONTAINER} container')
            return
            
        logging.info(f'Processing PDF: {file_name}')
        
        # Download blob content and metadata
        pdf_content, blob_metadata = download_blob_content_with_metadata(container_name, blob_name)
        
        if not pdf_content:
            logging.error(f'Failed to download blob: {blob_name}')
            return
        
        # Extract file_id from blob metadata or use CloudEvent ID
        file_id = ""
        if blob_metadata:
            file_id = blob_metadata.get('file_id', '')
        
        if not file_id:
            file_id = cloud_event.get('id', '')
            
        # Run async processing with enhanced timeout and retry logic
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Increased timeout for better handling of large files and network issues
            result = loop.run_until_complete(
                asyncio.wait_for(
                    process_pdf_classification(pdf_content, file_name, file_id, blob_metadata, cloud_event),
                    timeout=900.0  # 15 minutes timeout for complex operations
                )
            )
            
            if result['success']:
                logging.info(f'Successfully processed: {file_name}')
            else:
                logging.error(f'Failed to process: {file_name} - {result.get("error")}')
                raise Exception(f'Processing failed: {result.get("error")}')
                
        except asyncio.TimeoutError:
            logging.error(f'Processing timeout after 15 minutes: {file_name}')
            raise Exception(f'Processing timeout after 15 minutes: {file_name}')
        except Exception as e:
            logging.error(f'Processing error: {file_name} - {str(e)}')
            raise Exception(f'Processing error: {file_name} - {str(e)}')
        finally:
            try:
                if loop and not loop.is_closed():
                    loop.close()
            except Exception as cleanup_error:
                logging.warning(f'Error closing event loop: {cleanup_error}')
                pass
            
    except Exception as e:
        logging.error(f'CloudEvent processing error: {str(e)}')
        raise e

def download_blob_content_with_metadata(container_name: str, blob_name: str) -> tuple[Optional[bytes], Optional[Dict[str, str]]]:
    """Download blob content and metadata from storage"""
    try:
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        
        if not blob_client.exists():
            logging.error(f'Blob does not exist: {blob_name}')
            return None, None
        
        blob_properties = blob_client.get_blob_properties()
        blob_metadata = blob_properties.metadata if blob_properties.metadata else {}
        
        blob_data = blob_client.download_blob()
        content = blob_data.readall()
        
        return content, blob_metadata
        
    except Exception as e:
        logging.error(f'Error downloading blob {blob_name}: {str(e)}')
        return None, None

def download_blob_content(container_name: str, blob_name: str) -> Optional[bytes]:
    """Download blob content from storage (backward compatibility)"""
    content, _ = download_blob_content_with_metadata(container_name, blob_name)
    return content

async def process_pdf_classification(pdf_content: bytes, file_name: str, file_id: str, blob_metadata: Optional[Dict[str, str]] = None, cloud_event: Optional[Dict[str, Any]] = None) -> dict:
    """Process PDF through classification API and store results"""
    try:
        # Initialize authentication helper
        auth_helper = AuthHelper()
        
        # Perform login if credentials are configured
        if Config.LOGIN_URL and Config.LOGIN_EMAIL and Config.LOGIN_PASSWORD:
            login_success = auth_helper.login()
            if not login_success:
                logging.error('Login failed - continuing without authentication')
        
        # Call classification API
        classification_result = await call_classification_api(pdf_content, file_name)
        if not classification_result:
            logging.error(f'Classification API call failed for: {file_name}')
            return {'success': False, 'error': 'API call failed'}
        
        # Upload classified PDF with file_id
        upload_result = await upload_classified_pdf(pdf_content, file_name, classification_result, file_id, cloud_event)
        if not upload_result['success']:
            logging.error(f'Upload failed for: {file_name}')
            return upload_result
        
        # Save JSON result with file_id and enhanced metadata
        json_result = await save_classification_json(file_name, classification_result, file_id, blob_metadata, cloud_event)
        if not json_result['success']:
            logging.error(f'JSON save failed for: {file_name}')
            return json_result
        
        # Fetch and update metadata JSON from jsonfiles container
        updated_metadata_result = await fetch_and_update_metadata_json(
            file_id, 
            classification_result, 
            blob_metadata, 
            cloud_event, 
            upload_result.get('classified_url', '')
        )
        if not updated_metadata_result['success']:
            logging.warning(f'Metadata JSON update failed for file_id: {file_id}')
        
        # Update database with classification results via API if file_id exists
        if file_id and Config.TRANSACTION_URL:
            try:
                classified_pdf_url = upload_result.get('classified_url', '')
                
                # Use updated metadata if available, otherwise create basic payload
                if updated_metadata_result['success']:
                    wmt_payload = updated_metadata_result['updated_json']
                else:
                    # Create WMT payload for API call (fallback)
                    wmt_payload = {
                        "file_id": file_id,
                        "client_name": file_name,
                        "client_id": 714,
                        "process_code": Config.PROCESS_CODE,
                        "org_id": Config.ORG_ID,
                        "status": Config.STATUS,
                        "file_type": "PDF",
                        "file_open_date": datetime.utcnow().isoformat(),
                        "file_receive_date": datetime.utcnow().isoformat(),
                        "file_completion_date": datetime.utcnow().isoformat(),
                        "date_created": datetime.utcnow().isoformat(),
                        "template_info": {
                            "system_identified_document_category": classification_result.get('classification', ''),
                            "user_selected_document_category": "",
                            "category_confidence_score": classification_result.get('confidence_score', ''),
                            "original_bulkscan_pdf_url": cloud_event.get('data', {}).get('url', '') if cloud_event else '',
                            "extracted_split_pdf_file_url": classified_pdf_url,
                            "stamped_split_pdf_file_url": "",
                            "output_split_files_container_name": "",
                            "stamped_documents_container_name": "",
                            "document_classification_container_name": "",
                            "split_identifiers_provided_by_user": [],
                            "classification_identifiers_provided_by_user": [],
                            "extracted_page_numbers_from_ocr": [],
                            "manually_selected_page_numbers_by_user": []
                        }
                    }
                
                # Send to WMT Transaction API if authenticated
                if auth_helper.is_authenticated():
                    wmt_success = auth_helper.send_to_wmt_api(wmt_payload)
                    if not wmt_success:
                        logging.warning(f'WMT API update failed for file_id: {file_id}')
                else:
                    logging.warning('Not authenticated - skipping WMT API call')
                    
            except Exception as wmt_error:
                logging.error(f'WMT API error for file_id {file_id}: {str(wmt_error)}')
        
        return {
            'success': True,
            'classified_filename': upload_result['classified_filename'],
            'json_filename': json_result.get('json_filename'),
            'metadata_updated': updated_metadata_result['success'],
            'updated_metadata_filename': updated_metadata_result.get('json_filename', ''),
            'classification': classification_result.get('classification', 'unknown'),
            'confidence_score': classification_result.get('confidence_score', 0.0),
            'file_id': file_id,
            'cloud_event_id': cloud_event.get('id') if cloud_event else None,
            'json_stored': json_result['success'],
            'wmt_api_called': file_id and Config.TRANSACTION_URL and auth_helper.is_authenticated()
        }
        
    except Exception as e:
        logging.error(f'Error processing PDF: {str(e)}')
        return {'success': False, 'error': str(e)}

async def call_classification_api(pdf_content: bytes, file_name: str) -> Optional[Dict[str, Any]]:
    """Call the classification API"""
    try:
        if Config.CLASSIFICATION_API_CODE:
            api_url = f"{Config.CLASSIFICATION_API_URL}?code={Config.CLASSIFICATION_API_CODE}"
        else:
            api_url = Config.CLASSIFICATION_API_URL
        
        data = aiohttp.FormData()
        data.add_field('file', pdf_content, filename=file_name, content_type='application/pdf')
        
        # Enhanced timeout configuration for better reliability
        timeout = aiohttp.ClientTimeout(
            total=Config.CLASSIFICATION_API_TIMEOUT,  # Total timeout (default 300s)
            connect=60,     # Connection timeout (1 minute)
            sock_read=300,  # Socket read timeout (5 minutes)
            sock_connect=30 # Socket connect timeout (30 seconds)
        )
        
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(api_url, data=data) as response:
                if response.status == 200:
                    raw_response = await response.text()
                    
                    try:
                        json_data = json.loads(raw_response)
                        if not json_data:
                            logging.error(f'Empty JSON response received')
                            return None
                            
                        result = await handle_classification_response_direct(json_data)
                        if result:
                            logging.info(f'API call successful for: {file_name}')
                            return result
                        else:
                            logging.error(f'Failed to process classification response for: {file_name}')
                            return None
                    except json.JSONDecodeError as e:
                        logging.error(f'Failed to parse JSON response: {str(e)}')
                        return None
                else:
                    response_text = await response.text()
                    logging.error(f'API error {response.status}: {response_text}')
                    return None
                    
    except asyncio.TimeoutError:
        logging.error(f'API timeout for: {file_name}')
        return None
    except Exception as e:
        logging.error(f'API call error for {file_name}: {str(e)}')
        return None

async def handle_classification_response_direct(json_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Handle API response JSON data - supports both old and new API response formats"""
    try:
        # Handle new detailed API response format
        if 'json' in json_data and isinstance(json_data['json'], list) and len(json_data['json']) > 0:
            classification_data = json_data['json'][0]
            
            # Extract numeric confidence score from evaluation_verdict
            evaluation_verdict = classification_data.get('Evaluation Verdict', 'N/A')
            confidence_score = extract_confidence_score(evaluation_verdict)
            
            structured_result = {
                'classification': classification_data.get('Predicted Category', 'unknown'),
                'subcategory': classification_data.get('Predicted Subcategory', 'N/A'),
                'expected_category': classification_data.get('Expected Category', 'N/A'),
                'expected_subcategory': classification_data.get('Expected Subcategory', 'N/A'),
                'evaluation_type': classification_data.get('Evaluation Type', 'N/A'),
                'evaluation_verdict': evaluation_verdict,
                'evaluation_notes': classification_data.get('Evaluation Notes', 'N/A'),
                'classifier_reasoning': classification_data.get('Classifier Reasoning', 'N/A'),
                'classifier_keywords': classification_data.get('Classifier Keywords', 'N/A'),
                'file_name': classification_data.get('File Name', 'N/A'),
                'confidence_score': confidence_score,
                'full_response': json_data
            }
            
            return structured_result
        
        # Handle old detailed API response format
        elif 'classification_result' in json_data and 'json' in json_data['classification_result']:
            classification_data = json_data['classification_result']['json'][0] if json_data['classification_result']['json'] else {}
            
            # Extract numeric confidence score from evaluation_verdict
            evaluation_verdict = classification_data.get('Evaluation Verdict', 'N/A')
            confidence_score = extract_confidence_score(evaluation_verdict)
            
            structured_result = {
                'classification': classification_data.get('Predicted Category', 'unknown'),
                'subcategory': classification_data.get('Predicted Subcategory', 'N/A'),
                'expected_category': classification_data.get('Expected Category', 'N/A'),
                'expected_subcategory': classification_data.get('Expected Subcategory', 'N/A'),
                'evaluation_type': classification_data.get('Evaluation Type', 'N/A'),
                'evaluation_verdict': evaluation_verdict,
                'evaluation_notes': classification_data.get('Evaluation Notes', 'N/A'),
                'classifier_reasoning': classification_data.get('Classifier Reasoning', 'N/A'),
                'classifier_keywords': classification_data.get('Classifier Keywords', 'N/A'),
                'file_name': classification_data.get('File Name', 'N/A'),
                'confidence_score': confidence_score,
                'full_response': json_data
            }
            
            return structured_result
        
        # Handle old simple response format (backward compatibility)
        elif 'classification' in json_data:
            return json_data
        
        # If structure doesn't match any format, return as-is
        else:
            logging.warning(f'Unknown API response format: {json_data}')
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
        
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=Config.CLASSIFICATION_CONTAINER,
            blob=classified_filename
        )
        
        # Prepare metadata with all classification details
        metadata = {
            'classification': sanitize_metadata_value(str(classification)),
            'processed_at': sanitize_metadata_value(datetime.utcnow().isoformat()),
            'original_filename': sanitize_metadata_value(original_file_name)
        }
        
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
        
        for key, value in classification_fields.items():
            metadata[key] = sanitize_metadata_value(str(value))
        
        # Add CloudEvent information if available
        if cloud_event:
            metadata['cloud_event_id'] = sanitize_metadata_value(str(cloud_event.get('id', '')))
            metadata['cloud_event_type'] = sanitize_metadata_value(str(cloud_event.get('type', '')))
            metadata['cloud_event_time'] = sanitize_metadata_value(str(cloud_event.get('time', '')))
            metadata['cloud_event_source'] = sanitize_metadata_value(str(cloud_event.get('source', '')))
            
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
        
        classified_url = blob_client.url
        
        return {
            'success': True,
            'classified_filename': classified_filename,
            'classified_url': classified_url,
            'file_size': len(pdf_content),
            'file_id': file_id,
            'cloud_event_id': cloud_event.get('id') if cloud_event else None
        }
        
    except Exception as e:
        logging.error(f'Upload error: {str(e)}')
        return {'success': False, 'error': str(e)}

async def save_classification_json(original_file_name: str, classification_result: Dict[str, Any], file_id: str = "", blob_metadata: Optional[Dict[str, str]] = None, cloud_event: Optional[Dict[str, Any]] = None) -> dict:
    """Save JSON result with enhanced metadata"""
    try:
        name_without_ext = os.path.splitext(original_file_name)[0]
        json_filename = f"{name_without_ext}_classification_result.json"
        
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
        
        if file_id:
            json_data['file_id'] = file_id
            json_data['metadata']['file_id'] = file_id
        
        if blob_metadata:
            json_data['original_blob_metadata'] = blob_metadata
        
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
            container=Config.RESULTS_CONTAINER,
            blob=json_filename
        )
        
        # Prepare comprehensive metadata for the JSON blob
        blob_metadata_dict = {
            'content_type': 'application/json',
            'original_filename': sanitize_metadata_value(original_file_name),
            'processed_at': sanitize_metadata_value(datetime.utcnow().isoformat()),
            'classification': sanitize_metadata_value(str(classification_result.get('classification', 'unknown'))),
        }
        
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
        
        for key, value in classification_fields.items():
            blob_metadata_dict[key] = sanitize_metadata_value(str(value))
        
        # Add CloudEvent information to metadata if available
        if cloud_event:
            blob_metadata_dict['cloud_event_id'] = sanitize_metadata_value(str(cloud_event.get('id', '')))
            blob_metadata_dict['cloud_event_type'] = sanitize_metadata_value(str(cloud_event.get('type', '')))
            blob_metadata_dict['cloud_event_time'] = sanitize_metadata_value(str(cloud_event.get('time', '')))
            blob_metadata_dict['cloud_event_source'] = sanitize_metadata_value(str(cloud_event.get('source', '')))
            
            event_data = cloud_event.get('data', {})
            if event_data.get('contentType'):
                blob_metadata_dict['original_content_type'] = sanitize_metadata_value(str(event_data.get('contentType')))
            if event_data.get('contentLength'):
                blob_metadata_dict['original_content_length'] = sanitize_metadata_value(str(event_data.get('contentLength')))
            if event_data.get('eTag'):
                blob_metadata_dict['original_etag'] = sanitize_metadata_value(str(event_data.get('eTag')))
            if event_data.get('api'):
                blob_metadata_dict['blob_api'] = sanitize_metadata_value(str(event_data.get('api')))
        
        if 'evaluation_notes' in classification_result:
            blob_metadata_dict['evaluation_notes'] = sanitize_metadata_value(str(classification_result['evaluation_notes']))
        
        blob_client.upload_blob(
            data=json_bytes,
            content_settings=ContentSettings(content_type='application/json'),
            metadata=blob_metadata_dict,
            overwrite=True
        )
        
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
        logging.error(f'JSON save error: {str(e)}')
        return {'success': False, 'error': str(e)}

async def fetch_and_update_metadata_json(file_id: str, classification_result: Dict[str, Any], blob_metadata: Optional[Dict[str, str]] = None, cloud_event: Optional[Dict[str, Any]] = None, classified_pdf_url: str = "") -> dict:
    """Fetch JSON metadata from jsonfiles container and update with classification results"""
    try:
        if not file_id:
            return {'success': False, 'error': 'No file_id provided'}
        
        # Download existing JSON metadata from jsonfiles container
        json_filename = f"{file_id}.json"
        blob_service_client = get_blob_service_client()
        blob_client = blob_service_client.get_blob_client(
            container=Config.METADATA_CONTAINER,
            blob=json_filename
        )
        
        # Check if metadata JSON exists
        if not blob_client.exists():
            logging.warning(f'Metadata JSON not found: {json_filename}')
            return {'success': False, 'error': f'Metadata JSON not found: {json_filename}'}
        
        # Download and parse existing JSON
        blob_data = blob_client.download_blob()
        existing_json_content = blob_data.readall().decode('utf-8')
        existing_metadata = json.loads(existing_json_content)
        
        # Update the metadata with classification results and current timestamps
        current_time = datetime.utcnow().isoformat()
        
        # Update main fields
        existing_metadata.update({
            "client_id": 714,
            "process_code": Config.PROCESS_CODE,
            "org_id": Config.ORG_ID,
            "status": Config.STATUS,
            "file_type": "PDF",
            "file_open_date": current_time,
            "file_receive_date": current_time,
            "file_completion_date": current_time,
            "date_created": current_time
        })
        
        # Get original blob URL from cloud event if available
        original_blob_url = ""
        if cloud_event and cloud_event.get('data'):
            original_blob_url = cloud_event['data'].get('url', '') or cloud_event['data'].get('blobUrl', '')
        
        # Update template_info with new classification data
        template_info = existing_metadata.get('template_info', {})
        
        # Get storage account name dynamically
        storage_account_name = Config.get_storage_account_name()
        
        template_info.update({
            "system_identified_document_category": classification_result.get('classification', ''),
            "user_selected_document_category": "",
            "category_confidence_score": classification_result.get('confidence_score', ''),
            "original_bulkscan_pdf_url": original_blob_url,
            "extracted_split_pdf_file_url": classified_pdf_url,  # Classified PDF URL
            "stamped_split_pdf_file_url": original_blob_url,  # Input PDF file blob URL
            "output_split_files_container_name": f"https://{storage_account_name}.blob.core.windows.net/{Config.CLASSIFICATION_CONTAINER}/",  # Classification location PDF URL
            "stamped_documents_container_name": Config.INPUT_CONTAINER,  # Container name of the input container
            "document_classification_container_name": Config.CLASSIFICATION_CONTAINER,  # PDF output container name
            "split_identifiers_provided_by_user": [],
            "classification_identifiers_provided_by_user": []
        })
        
        # Keep existing page numbers if they exist, otherwise initialize empty arrays
        if "extracted_page_numbers_from_ocr" not in template_info:
            template_info["extracted_page_numbers_from_ocr"] = []
        if "manually_selected_page_numbers_by_user" not in template_info:
            template_info["manually_selected_page_numbers_by_user"] = []
        if "enhanced_split_identifiers_by_range" not in template_info:
            template_info["enhanced_split_identifiers_by_range"] = []
        
        existing_metadata["template_info"] = template_info
        
        # Convert updated metadata to JSON bytes
        updated_json_content = json.dumps(existing_metadata, indent=2, ensure_ascii=False)
        updated_json_bytes = updated_json_content.encode('utf-8')
        
        # Upload updated JSON back to metadata container (update original)
        metadata_blob_client = blob_service_client.get_blob_client(
            container=Config.METADATA_CONTAINER,
            blob=json_filename
        )
        
        metadata_blob_client.upload_blob(
            data=updated_json_bytes,
            content_settings=ContentSettings(content_type='application/json'),
            metadata={
                'file_id': sanitize_metadata_value(str(file_id)),
                'updated_at': sanitize_metadata_value(current_time),
                'classification': sanitize_metadata_value(str(classification_result.get('classification', ''))),
                'content_type': 'application/json'
            },
            overwrite=True
        )
        
        # Also save to results container for backup/history
        results_blob_client = blob_service_client.get_blob_client(
            container=Config.RESULTS_CONTAINER,
            blob=f"{file_id}_updated_metadata.json"
        )
        
        results_blob_client.upload_blob(
            data=updated_json_bytes,
            content_settings=ContentSettings(content_type='application/json'),
            metadata={
                'file_id': sanitize_metadata_value(str(file_id)),
                'updated_at': sanitize_metadata_value(current_time),
                'classification': sanitize_metadata_value(str(classification_result.get('classification', ''))),
                'content_type': 'application/json'
            },
            overwrite=True
        )
        
        logging.info(f'Updated metadata JSON for file_id: {file_id}')
        
        return {
            'success': True,
            'updated_json': existing_metadata,
            'json_filename': f"{file_id}_updated_metadata.json",
            'file_size': len(updated_json_bytes)
        }
        
    except json.JSONDecodeError as e:
        logging.error(f'Failed to parse existing JSON metadata for file_id {file_id}: {str(e)}')
        return {'success': False, 'error': f'Invalid JSON format: {str(e)}'}
    except Exception as e:
        logging.error(f'Error updating metadata JSON for file_id {file_id}: {str(e)}')
        return {'success': False, 'error': str(e)}

