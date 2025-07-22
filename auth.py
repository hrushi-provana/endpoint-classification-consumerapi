"""
Authentication Helper
Handles login and token management
"""
import requests
import logging
from typing import Optional
from config import Config

class AuthHelper:
    def __init__(self):
        self.token = None
        self.session = requests.Session()
        
    def login(self) -> bool:
        """
        Perform login to WMT API to get authentication token
        
        Returns:
            bool: True if login successful, False otherwise
        """
        try:
            login_payload = {
                'emailAddress': Config.LOGIN_EMAIL,
                'password': Config.LOGIN_PASSWORD
            }
            
            response = self.session.post(
                Config.LOGIN_URL,
                json=login_payload,
                timeout=Config.API_REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                data = response.json()
                # Extract accessToken from the nested response structure
                if data.get('isSuccess') and data.get('data') and data.get('data').get('tokens'):
                    self.token = data['data']['tokens'].get('accessToken')
                else:
                    # Fallback to direct access
                    self.token = data.get('accessToken')
                
                if self.token:
                    # Set token in session headers for future requests
                    self.session.headers.update({
                        'Authorization': f'Bearer {self.token}',
                        'Content-Type': 'application/json'
                    })
                    logging.info("Login successful")
                    return True
                else:
                    logging.error("Login response missing accessToken")
                    logging.error(f"Response structure: {data}")
                    return False
            else:
                logging.error(f"Login failed with status: {response.status_code}")
                logging.error(f"Response: {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Login request error: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected login error: {e}")
            return False
            
    def get_authenticated_session(self) -> Optional[requests.Session]:
        """
        Get authenticated session with token
        
        Returns:
            requests.Session: Authenticated session or None if not authenticated
        """
        if self.token:
            return self.session
        else:
            logging.warning("No authentication token available")
            return None
            
    def is_authenticated(self) -> bool:
        """Check if currently authenticated"""
        return self.token is not None
        
    def send_to_wmt_api(self, wmt_payload: dict) -> bool:
        """
        Send data to WMT Transaction API with upsert logic (update if exists, create if not)
        
        Args:
            wmt_payload: JSON payload to send to WMT API
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.is_authenticated():
                logging.error("Not authenticated - cannot send to WMT API")
                return False
                
            file_id = wmt_payload.get('file_id')
            if not file_id:
                logging.error("No file_id in payload - cannot proceed with WMT API")
                return False
            
            # Step 1: Check if record exists
            existing_record = self.check_record_exists(file_id)
            
            if existing_record:
                # Step 2a: Update existing record
                logging.info(f"Record exists for file_id {file_id}, updating...")
                return self.update_wmt_record(file_id, wmt_payload)
            else:
                # Step 2b: Create new record
                logging.info(f"No record found for file_id {file_id}, creating new...")
                return self.create_wmt_record(wmt_payload)
                
        except requests.exceptions.RequestException as e:
            logging.error(f"WMT API request error: {e}")
            return False
        except Exception as e:
            logging.error(f"Unexpected WMT API error: {e}")
            return False
    
    def check_record_exists(self, file_id: str) -> bool:
        """
        Check if a record exists in WMT API by file_id
        
        Args:
            file_id: The file ID to check
            
        Returns:
            bool: True if record exists, False otherwise
        """
        try:
            # GET request to check if record exists
            check_url = f"{Config.TRANSACTION_URL}/{file_id}"
            
            response = self.session.get(
                check_url,
                timeout=Config.WMT_API_TIMEOUT
            )
            
            if response.status_code == 200:
                logging.info(f"Record found for file_id: {file_id}")
                return True
            elif response.status_code == 404:
                logging.info(f"No record found for file_id: {file_id}")
                return False
            else:
                logging.warning(f"Unexpected response checking file_id {file_id}: {response.status_code}")
                return False
                
        except requests.exceptions.Timeout:
            logging.error(f"Timeout checking record for file_id: {file_id}")
            return False
        except requests.exceptions.RequestException as e:
            logging.error(f"Error checking record for file_id {file_id}: {e}")
            return False
    
    def create_wmt_record(self, wmt_payload: dict) -> bool:
        """
        Create a new record in WMT API
        
        Args:
            wmt_payload: JSON payload to send to WMT API
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = self.session.post(
                Config.TRANSACTION_URL,
                json=wmt_payload,
                timeout=Config.WMT_API_TIMEOUT
            )
            
            if response.status_code in [200, 201]:
                logging.info("Successfully created new record in WMT Transaction API")
                return True
            else:
                logging.error(f"WMT API create error {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            logging.error("Timeout creating WMT record")
            return False
        except requests.exceptions.RequestException as e:
            logging.error(f"WMT API create request error: {e}")
            return False
    
    def update_wmt_record(self, file_id: str, wmt_payload: dict) -> bool:
        """
        Update an existing record in WMT API
        
        Args:
            file_id: The file ID to update
            wmt_payload: JSON payload to send to WMT API
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # PUT request to update existing record
            update_url = f"{Config.TRANSACTION_URL}/{file_id}"
            
            response = self.session.put(
                update_url,
                json=wmt_payload,
                timeout=Config.WMT_API_TIMEOUT
            )
            
            if response.status_code in [200, 204]:
                logging.info(f"Successfully updated record for file_id: {file_id}")
                return True
            else:
                logging.error(f"WMT API update error {response.status_code}: {response.text}")
                return False
                
        except requests.exceptions.Timeout:
            logging.error(f"Timeout updating WMT record for file_id: {file_id}")
            return False
        except requests.exceptions.RequestException as e:
            logging.error(f"WMT API update request error for file_id {file_id}: {e}")
            return False
