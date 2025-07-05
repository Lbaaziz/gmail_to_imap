#!/usr/bin/env python3
"""
Gmail API client for Gmail to IMAP transfer system.
"""

import os
import time
import logging
from typing import Dict, List, Any

# Google API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


class GmailClient:
    """Handles Gmail API interactions."""
    
    SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
    
    def __init__(self, credentials_file: str):
        self.credentials_file = credentials_file
        self.service = None
        self.authenticate()
    
    def authenticate(self) -> None:
        """Authenticate with Gmail using OAuth 2.0."""
        creds = None
        token_file = 'token.json'
        
        # Load existing token
        if os.path.exists(token_file):
            creds = Credentials.from_authorized_user_file(token_file, self.SCOPES)
        
        # If no valid credentials, get new ones
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logging.warning(f"Token refresh failed: {e}")
                    creds = None
            
            if not creds:
                if not os.path.exists(self.credentials_file):
                    raise FileNotFoundError(f"Gmail credentials file '{self.credentials_file}' not found")
                
                # Add diagnostic logging for OAuth debugging
                logging.info("Starting OAuth flow...")
                
                # Load credentials file to check configuration
                import json
                try:
                    with open(self.credentials_file, 'r') as f:
                        cred_data = json.load(f)
                    
                    # Check if it's a desktop app configuration
                    if 'installed' in cred_data:
                        app_type = 'installed'
                        redirect_uris = cred_data['installed'].get('redirect_uris', [])
                        logging.info(f"OAuth app type: {app_type}")
                        logging.info(f"Configured redirect URIs: {redirect_uris}")
                    elif 'web' in cred_data:
                        app_type = 'web'
                        redirect_uris = cred_data['web'].get('redirect_uris', [])
                        logging.info(f"OAuth app type: {app_type}")
                        logging.info(f"Configured redirect URIs: {redirect_uris}")
                        logging.warning("Web app type detected - this may cause redirect issues for desktop apps")
                    else:
                        logging.error("Unknown OAuth app configuration")
                        
                except Exception as e:
                    logging.error(f"Could not parse credentials file: {e}")
                
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_file, self.SCOPES)
                
                # Try to determine appropriate port based on redirect URIs
                port = 8085  # Default to random port
                if 'installed' in cred_data:
                    redirect_uris = cred_data['installed'].get('redirect_uris', [])
                    for uri in redirect_uris:
                        if 'localhost' in uri and ':' in uri:
                            try:
                                port = int(uri.split(':')[-1].split('/')[0])
                                logging.info(f"Using configured port: {port}")
                                break
                            except (ValueError, IndexError):
                                pass
                
                if port == 0:
                    logging.info("Using random port for OAuth callback")
                else:
                    logging.info(f"Using fixed port {port} for OAuth callback")
                
                logging.info("Opening browser for OAuth authorization...")
                creds = flow.run_local_server(port=port)
                logging.info("OAuth flow completed successfully")
            
            # Save credentials for next run
            with open(token_file, 'w') as token:
                token.write(creds.to_json())
        
        self.service = build('gmail', 'v1', credentials=creds)
        logging.info("Gmail authentication successful")
    
    def get_labels(self) -> List[Dict[str, str]]:
        """Get all Gmail labels."""
        try:
            results = self.service.users().labels().list(userId='me').execute()
            labels = results.get('labels', [])
            logging.info(f"Found {len(labels)} Gmail labels")
            return labels
        except HttpError as e:
            logging.error(f"Failed to get Gmail labels: {e}")
            raise
    
    def get_messages_by_label(self, label_id: str) -> List[str]:
        """Get all message IDs for a specific label."""
        try:
            messages = []
            page_token = None
            
            while True:
                results = self.service.users().messages().list(
                    userId='me',
                    labelIds=[label_id],
                    pageToken=page_token
                ).execute()
                
                batch_messages = results.get('messages', [])
                messages.extend([msg['id'] for msg in batch_messages])
                
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            
            logging.info(f"Found {len(messages)} messages for label {label_id}")
            return messages
        except HttpError as e:
            logging.error(f"Failed to get messages for label {label_id}: {e}")
            raise
    
    def get_message(self, message_id: str) -> Dict[str, Any]:
        """Get full message details."""
        try:
            message = self.service.users().messages().get(
                userId='me',
                id=message_id,
                format='raw'
            ).execute()
            return message
        except HttpError as e:
            logging.error(f"Failed to get message {message_id}: {e}")
            raise
    
    def get_messages_batch(self, message_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get multiple messages in a single batch request with rate limiting and retry logic."""
        if not message_ids:
            return {}
        
        # Reduce batch size to be more conservative with rate limits
        max_batch_size = 25  # Reduced from 50 to avoid rate limiting
        all_messages = {}
        
        # Process in chunks of max_batch_size
        for i in range(0, len(message_ids), max_batch_size):
            batch_ids = message_ids[i:i + max_batch_size]
            
            # Retry logic for rate limiting
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    def batch_callback(request_id, response, exception):
                        """Callback function for batch request."""
                        if exception is not None:
                            # Check if it's a rate limiting error
                            if hasattr(exception, 'resp') and exception.resp.status == 429:
                                logging.warning(f"Rate limited for message {request_id}, will retry batch")
                            else:
                                logging.error(f"Failed to get message {request_id}: {exception}")
                        else:
                            all_messages[request_id] = response
                    
                    # Use modern service-specific batch endpoint
                    batch = self.service.new_batch_http_request(callback=batch_callback)
                    
                    # Add individual message requests to batch
                    for msg_id in batch_ids:
                        request = self.service.users().messages().get(
                            userId='me',
                            id=msg_id,
                            format='raw'
                        )
                        batch.add(request, request_id=msg_id)
                    
                    # Execute batch request
                    batch.execute()
                    
                    # Check if we got rate limited responses
                    rate_limited_count = len(batch_ids) - len([m for m in all_messages.keys() if m in batch_ids])
                    
                    if rate_limited_count > 0 and attempt < max_retries - 1:
                        # Some requests were rate limited, wait and retry
                        wait_time = (2 ** attempt) * 5  # Exponential backoff: 5s, 10s, 20s
                        logging.warning(f"Rate limited on {rate_limited_count} requests, waiting {wait_time}s before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        
                        # Reset batch for rate limited messages only
                        rate_limited_ids = [msg_id for msg_id in batch_ids if msg_id not in all_messages]
                        batch_ids = rate_limited_ids
                        continue
                    else:
                        # Success or final attempt
                        successful_count = len([m for m in all_messages.keys() if m in batch_ids])
                        logging.info(f"Batch fetched {len(batch_ids)} messages (got {successful_count} responses)")
                        break
                        
                except HttpError as e:
                    if e.resp.status == 429 and attempt < max_retries - 1:
                        # Rate limited at batch level, wait and retry
                        wait_time = (2 ** attempt) * 10  # Longer wait for batch-level rate limiting
                        logging.warning(f"Batch rate limited (attempt {attempt + 1}/{max_retries}), waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logging.error(f"Batch request failed: {e}")
                        # Fallback to individual requests for this batch with rate limiting
                        for msg_id in batch_ids:
                            if msg_id not in all_messages:  # Skip already fetched messages
                                for individual_attempt in range(3):
                                    try:
                                        message = self.get_message(msg_id)
                                        all_messages[msg_id] = message
                                        break
                                    except HttpError as individual_error:
                                        if individual_error.resp.status == 429 and individual_attempt < 2:
                                            individual_wait = (2 ** individual_attempt) * 2
                                            logging.warning(f"Individual message {msg_id} rate limited, waiting {individual_wait}s...")
                                            time.sleep(individual_wait)
                                        else:
                                            logging.error(f"Failed to get message {msg_id} individually: {individual_error}")
                                            break
                        break
            
            # Add delay between batches to respect rate limits
            if i + max_batch_size < len(message_ids):  # Not the last batch
                time.sleep(2)  # 2 second delay between batches
        
        logging.info(f"Total messages fetched in batches: {len(all_messages)}")
        return all_messages