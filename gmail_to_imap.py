#!/usr/bin/env python3
"""
Gmail to IMAP Transfer System

A simplified Python-based system that transfers emails from Gmail to any IMAP server
while preserving folder structure, metadata, and content. Supports progress tracking
and resumable transfers.
"""

import os
import json
import time
import yaml
import logging
import threading
import queue
from datetime import datetime
from typing import Dict, List, Optional, Any
import psutil
import signal

# Google API imports
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# IMAP imports
import imapclient
import email
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Progress bar
from tqdm import tqdm


class ConfigManager:
    """Handles configuration loading and validation."""
    
    def __init__(self, config_file: str = "config.yaml"):
        self.config_file = config_file
        self.config = self.load_config()
    
    def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        try:
            with open(self.config_file, 'r') as file:
                config = yaml.safe_load(file)
            self.validate_config(config)
            return config
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file '{self.config_file}' not found")
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in configuration file: {e}")
    
    def validate_config(self, config: Dict[str, Any]) -> None:
        """Validate configuration structure."""
        required_sections = ['gmail', 'imap', 'settings']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate Gmail config
        gmail_config = config['gmail']
        if 'credentials_file' not in gmail_config:
            raise ValueError("Missing 'credentials_file' in gmail configuration")
        
        # Validate IMAP config
        imap_config = config['imap']
        required_imap_fields = ['server', 'port', 'username', 'password']
        for field in required_imap_fields:
            if field not in imap_config:
                raise ValueError(f"Missing required IMAP field: {field}")


class ProgressManager:
    """Handles progress tracking and resumable transfers."""
    
    def __init__(self, progress_file: str = "progress.json"):
        self.progress_file = progress_file
        self.progress = self.load_progress()
    
    def load_progress(self) -> Dict[str, Any]:
        """Load progress from JSON file."""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as file:
                    return json.load(file)
            except (json.JSONDecodeError, IOError):
                logging.warning(f"Could not load progress file, starting fresh")
        
        # Initialize new progress
        return {
            "session_id": datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
            "total_labels": 0,
            "completed_labels": 0,
            "current_label": "",
            "transferred_messages": {},
            "label_folder_mapping": {}
        }
    
    def save_progress(self) -> None:
        """Save progress to JSON file."""
        try:
            with open(self.progress_file, 'w') as file:
                json.dump(self.progress, file, indent=2)
        except IOError as e:
            logging.error(f"Failed to save progress: {e}")
    
    def is_message_transferred(self, message_id: str, label: str) -> bool:
        """Check if a message has already been transferred."""
        return (label in self.progress.get("transferred_messages", {}) and 
                message_id in self.progress["transferred_messages"][label])
    
    def mark_message_completed(self, message_id: str, label: str) -> None:
        """Mark a message as completed."""
        if "transferred_messages" not in self.progress:
            self.progress["transferred_messages"] = {}
        if label not in self.progress["transferred_messages"]:
            self.progress["transferred_messages"][label] = []
        
        if message_id not in self.progress["transferred_messages"][label]:
            self.progress["transferred_messages"][label].append(message_id)
        
        # Don't save immediately - batch the saves
        
    def save_progress_batch(self, force: bool = False) -> None:
        """Save progress in batches to reduce I/O."""
        if not hasattr(self, '_last_save_time'):
            self._last_save_time = 0
        
        current_time = time.time()
        # Save every 30 seconds or when forced
        if force or (current_time - self._last_save_time) >= 30:
            self.save_progress()
            self._last_save_time = current_time
    
    def is_label_completed(self, label: str) -> bool:
        """Check if a label has been completely processed."""
        return self.progress.get("current_label") != label and \
               label in self.progress.get("transferred_messages", {})


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


class IMAPClient:
    """Handles IMAP server operations with SSL stability."""
    
    def __init__(self, server: str, port: int, username: str, password: str, use_ssl: bool = True):
        self.server = server
        self.port = port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl
        self.client = None
        self.connection_start_time = None
        self.connection_errors = 0
        self.last_activity = None
        self.total_uploads = 0
        self.max_connection_duration = 900  # 15 minutes max connection time
        self.max_uploads_per_connection = 100  # Max uploads before reconnect
        self.connect()
    
    def connect(self) -> None:
        """Connect to IMAP server with health monitoring."""
        try:
            self.connection_start_time = time.time()
            logging.info(f"🔌 Attempting IMAP connection to {self.server}:{self.port}")
            
            self.client = imapclient.IMAPClient(self.server, port=self.port, ssl=self.use_ssl)
            self.client.login(self.username, self.password)
            self.last_activity = time.time()
            
            logging.info(f"✅ Connected to IMAP server {self.server}")
            logging.info(f"🔗 Connection established in {self.last_activity - self.connection_start_time:.2f}s")
            
            # Diagnostic: Check server capabilities and namespaces
            try:
                capabilities = self.client.capabilities()
                logging.info(f"IMAP server capabilities: {list(capabilities)}")
                
                # Check namespace support
                if b'NAMESPACE' in capabilities:
                    namespace = self.client.namespace()
                    logging.info(f"IMAP namespaces: {namespace}")
                    
                    # Extract personal namespace prefix
                    if namespace and namespace[0]:
                        personal_ns = namespace[0][0]
                        if personal_ns:
                            self.namespace_prefix = personal_ns[0] if personal_ns[0] else ""
                            self.namespace_delimiter = personal_ns[1] if personal_ns[1] else "."
                            logging.info(f"Personal namespace prefix: '{self.namespace_prefix}', delimiter: '{self.namespace_delimiter}'")
                        else:
                            self.namespace_prefix = ""
                            self.namespace_delimiter = "."
                    else:
                        self.namespace_prefix = ""
                        self.namespace_delimiter = "."
                else:
                    logging.warning("Server does not support NAMESPACE command")
                    # Default assumption for most IMAP servers
                    self.namespace_prefix = "INBOX."
                    self.namespace_delimiter = "."
                    
            except Exception as e:
                logging.warning(f"Could not get namespace info: {e}")
                # Default assumption for most IMAP servers that require INBOX prefix
                self.namespace_prefix = "INBOX."
                self.namespace_delimiter = "."
                
            logging.info(f"Using namespace prefix: '{self.namespace_prefix}' with delimiter: '{self.namespace_delimiter}'")
                
        except Exception as e:
            logging.error(f"Failed to connect to IMAP server: {e}")
            raise
    
    def create_folder(self, folder_name: str) -> None:
        """Create folder if it doesn't exist."""
        try:
            # Apply namespace prefix if needed
            full_folder_name = self._get_full_folder_name(folder_name)
            
            if not self.client.folder_exists(full_folder_name):
                self.client.create_folder(full_folder_name)
                logging.info(f"Created IMAP folder: {full_folder_name}")
            else:
                logging.info(f"IMAP folder already exists: {full_folder_name}")
        except Exception as e:
            logging.error(f"Failed to create folder {folder_name} (full name: {self._get_full_folder_name(folder_name)}): {e}")
            raise
    
    def _get_full_folder_name(self, folder_name: str) -> str:
        """Get full folder name with namespace prefix."""
        # Don't prefix INBOX itself
        if folder_name.upper() == 'INBOX':
            return 'INBOX'
        
        # If folder already has the prefix, don't add it again
        if hasattr(self, 'namespace_prefix') and self.namespace_prefix:
            if folder_name.startswith(self.namespace_prefix):
                return folder_name
            return f"{self.namespace_prefix}{folder_name}"
        else:
            # Default behavior for servers without namespace info
            if folder_name.startswith('INBOX.'):
                return folder_name
            return f"INBOX.{folder_name}"
    
    def upload_message(self, folder_name: str, message_data: bytes, flags: List[str] = None, msg_time: datetime = None) -> None:
        """Upload message to IMAP folder with SSL stability and connection recycling."""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                if flags is None:
                    flags = []
                
                # Check if connection needs recycling BEFORE upload
                if self._should_recycle_connection():
                    logging.info("🔄 Recycling IMAP connection for SSL stability")
                    self._reconnect()
                
                # Check connection health before upload
                self._check_connection_health()
                
                # Apply namespace prefix if needed
                full_folder_name = self._get_full_folder_name(folder_name)
                
                # Track activity
                start_time = time.time()
                self.client.append(full_folder_name, message_data, flags, msg_time)
                self.last_activity = time.time()
                self.total_uploads += 1
                
                # Log slow uploads
                upload_time = self.last_activity - start_time
                if upload_time > 5.0:  # More than 5 seconds
                    logging.warning(f"⚠️ Slow IMAP upload: {upload_time:.2f}s for message to {folder_name}")
                
                return  # Success - exit retry loop
                
            except Exception as e:
                self.connection_errors += 1
                full_folder_name = self._get_full_folder_name(folder_name)
                
                # Check if this is an SSL/connection error that should trigger reconnection
                is_ssl_error = ("SSL" in str(e) or "socket" in str(e).lower() or
                               "LOGOUT" in str(e) or "connection" in str(e).lower())
                
                if is_ssl_error:
                    logging.error(f"🔌 IMAP connection error #{self.connection_errors}: {e}")
                    self._log_connection_diagnostics()
                    
                    # Try to reconnect for SSL errors (except on last attempt)
                    if attempt < max_retries - 1:
                        logging.info(f"🔄 Attempting reconnection (attempt {attempt + 1}/{max_retries})")
                        try:
                            self._reconnect()
                            time.sleep(1)  # Brief pause before retry
                            continue
                        except Exception as reconnect_error:
                            logging.error(f"❌ Reconnection failed: {reconnect_error}")
                else:
                    logging.error(f"Failed to upload message to {folder_name} (full name: {full_folder_name}): {e}")
                
                # If this is the last attempt or not an SSL error, re-raise
                if attempt == max_retries - 1:
                    raise
    
    def _should_recycle_connection(self) -> bool:
        """Check if connection should be recycled for SSL stability."""
        if not self.connection_start_time:
            return False
            
        # Check connection duration
        connection_age = time.time() - self.connection_start_time
        if connection_age > self.max_connection_duration:
            logging.info(f"🕒 Connection recycling: age {connection_age:.1f}s > {self.max_connection_duration}s")
            return True
        
        # Check upload count
        if self.total_uploads >= self.max_uploads_per_connection:
            logging.info(f"📊 Connection recycling: {self.total_uploads} uploads >= {self.max_uploads_per_connection}")
            return True
        
        # Check error rate
        if self.connection_errors >= 10:  # Too many errors
            logging.info(f"❌ Connection recycling: {self.connection_errors} errors")
            return True
            
        return False
    
    def _reconnect(self) -> None:
        """Safely reconnect to IMAP server."""
        try:
            # Close existing connection
            if self.client:
                try:
                    self.client.logout()
                except:
                    pass  # Ignore errors on logout
                self.client = None
            
            # Reset counters
            old_errors = self.connection_errors
            self.connection_errors = 0
            self.total_uploads = 0
            
            # Reconnect
            self.connect()
            logging.info(f"✅ IMAP reconnection successful (previous errors: {old_errors})")
            
        except Exception as e:
            logging.error(f"❌ IMAP reconnection failed: {e}")
            raise
    
    def _check_connection_health(self) -> None:
        """Check IMAP connection health."""
        if self.last_activity:
            time_since_activity = time.time() - self.last_activity
            if time_since_activity > 300:  # 5 minutes of inactivity
                logging.warning(f"⚠️ IMAP connection inactive for {time_since_activity:.1f}s")
    
    def _log_connection_diagnostics(self) -> None:
        """Log detailed connection diagnostics."""
        if self.connection_start_time:
            connection_duration = time.time() - self.connection_start_time
            logging.info(f"🔗 Connection duration: {connection_duration:.1f}s")
            logging.info(f"❌ Connection errors: {self.connection_errors}")
            
            if self.last_activity:
                time_since_activity = time.time() - self.last_activity
                logging.info(f"⏱️ Time since last activity: {time_since_activity:.1f}s")
    
    def disconnect(self) -> None:
        """Disconnect from IMAP server with diagnostics."""
        if self.client:
            try:
                self.client.logout()
                if self.connection_start_time:
                    total_duration = time.time() - self.connection_start_time
                    logging.info(f"✅ Disconnected from IMAP server (duration: {total_duration:.1f}s, errors: {self.connection_errors})")
                else:
                    logging.info("✅ Disconnected from IMAP server")
            except Exception as e:
                logging.error(f"❌ Error disconnecting from IMAP server: {e}")
                self._log_connection_diagnostics()


def safe_transfer(func, max_retries: int = 3):
    """Decorator for safe transfer operations with retry logic."""
    def wrapper(*args, **kwargs):
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                wait_time = 2 ** attempt
                logging.warning(f"Transfer attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
    return wrapper


class GmailToImapTransfer:
    """Main transfer orchestrator."""
    
    def __init__(self, config_file: str = "config.yaml"):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('gmail_to_imap.log'),
                logging.StreamHandler()
            ]
        )
        
        self.config_manager = ConfigManager(config_file)
        self.config = self.config_manager.config
        self.progress_manager = ProgressManager()
        self.gmail_client = None
        self.imap_client = None
        
        # Message cache for deduplication - avoid fetching same message multiple times
        self.message_cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        
        # Thread management and shutdown handling
        self.active_threads = []
        self.shutdown_requested = False
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        def signal_handler(signum, frame):
            logging.info(f"🛑 Received signal {signum}, initiating graceful shutdown...")
            self.shutdown_requested = True
            
            # Notify all active threads to stop
            for thread_info in self.active_threads:
                if 'stop_event' in thread_info:
                    thread_info['stop_event'].set()
        
        # Register handlers for common termination signals
        signal.signal(signal.SIGINT, signal_handler)   # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # Termination request
    
    def setup_clients(self) -> None:
        """Initialize Gmail and IMAP clients."""
        # Setup Gmail client
        gmail_config = self.config['gmail']
        self.gmail_client = GmailClient(gmail_config['credentials_file'])
        
        # Setup IMAP client
        imap_config = self.config['imap']
        self.imap_client = IMAPClient(
            server=imap_config['server'],
            port=imap_config['port'],
            username=imap_config['username'],
            password=imap_config['password'],
            use_ssl=imap_config.get('use_ssl', True)
        )
    
    def create_folder_mappings(self, labels: List[Dict[str, str]]) -> Dict[str, str]:
        """Create mapping from Gmail labels to IMAP folders."""
        label_mappings = self.config['settings'].get('label_mappings', {})
        folder_mapping = {}
        
        # Log mapping strategy
        configured_labels = set(label_mappings.keys())
        all_label_names = {label['name'] for label in labels}
        dynamic_labels = all_label_names - configured_labels
        
        logging.info(f"Processing {len(labels)} Gmail labels for folder mapping:")
        logging.info(f"- {len(configured_labels)} labels have custom mappings: {sorted(configured_labels)}")
        logging.info(f"- {len(dynamic_labels)} labels will be created dynamically: {sorted(dynamic_labels)}")
        
        for label in labels:
            label_name = label['name']
            label_id = label['id']
            
            # Use custom mapping if available, otherwise use label name
            if label_name in label_mappings:
                folder_name = label_mappings[label_name]
                mapping_type = "configured"
                logging.info(f"Label '{label_name}' -> '{folder_name}' (configured mapping)")
            else:
                # Clean label name for folder use
                folder_name = label_name.replace('/', '_').replace('\\', '_')
                # Additional cleaning for special characters
                folder_name = folder_name.replace('[Gmail]/', '').strip()
                mapping_type = "dynamic"
                logging.info(f"Label '{label_name}' -> '{folder_name}' (dynamic creation)")
            
            folder_mapping[label_id] = folder_name
            
            # Create folder on IMAP server
            try:
                self.imap_client.create_folder(folder_name)
                logging.info(f"✓ Folder '{folder_name}' ready for label '{label_name}'")
            except Exception as e:
                logging.error(f"✗ Failed to create folder '{folder_name}' for label '{label_name}': {e}")
                raise
        
        # Summary logging
        logging.info(f"Folder mapping completed:")
        logging.info(f"- Total labels processed: {len(labels)}")
        logging.info(f"- Folders created/verified: {len(folder_mapping)}")
        
        # Save mapping to progress
        self.progress_manager.progress['label_folder_mapping'] = folder_mapping
        self.progress_manager.save_progress()
        
        return folder_mapping
    
    @safe_transfer
    def transfer_message(self, message_id: str, label_id: str, folder_name: str) -> None:
        """Transfer a single message from Gmail to IMAP (legacy method - use transfer_message_from_cache for better performance)."""
        
        # Check cache first to avoid redundant fetches
        if message_id in self.message_cache:
            cached_data = self.message_cache[message_id]
            raw_message = cached_data['raw_message']
            flags = cached_data['flags']
            msg_time = cached_data['msg_time']
            self.cache_hits += 1
        else:
            # Get message from Gmail
            message_data = self.gmail_client.get_message(message_id)
            
            # Decode raw message
            import base64
            raw_message = base64.urlsafe_b64decode(message_data['raw'])
            
            # Parse email to get metadata
            email_message = email.message_from_bytes(raw_message)
            
            # Extract flags (basic implementation)
            flags = []
            labels = message_data.get('labelIds', [])
            if 'UNREAD' not in labels:
                flags.append('\\Seen')
            if 'STARRED' in labels:
                flags.append('\\Flagged')
            
            # Get message date
            date_header = email_message.get('Date')
            msg_time = None
            if date_header:
                try:
                    from email.utils import parsedate_to_datetime
                    msg_time = parsedate_to_datetime(date_header)
                except:
                    pass
            
            # Cache the message data for potential reuse
            self.message_cache[message_id] = {
                'raw_message': raw_message,
                'flags': flags,
                'msg_time': msg_time
            }
            self.cache_misses += 1
        
        # Upload to IMAP
        self.imap_client.upload_message(folder_name, raw_message, flags, msg_time)
        
        # Mark as completed
        self.progress_manager.mark_message_completed(message_id, label_id)
    
    @safe_transfer
    def transfer_message_from_cache(self, message_id: str, label_id: str, folder_name: str) -> None:
        """Transfer a message from cache (assumes message is already cached via batch fetch)."""
        
        # Get cached message data
        if message_id not in self.message_cache:
            # Fallback to individual fetch if not in cache
            logging.warning(f"Message {message_id} not in cache, falling back to individual fetch")
            return self.transfer_message(message_id, label_id, folder_name)
        
        cached_data = self.message_cache[message_id]
        raw_message = cached_data['raw_message']
        flags = cached_data['flags']
        msg_time = cached_data['msg_time']
        self.cache_hits += 1
        
        # Upload to IMAP
        self.imap_client.upload_message(folder_name, raw_message, flags, msg_time)
        
        # Mark as completed
        self.progress_manager.mark_message_completed(message_id, label_id)
    
    def transfer_label(self, label: Dict[str, str], folder_mapping: Dict[str, str]) -> None:
        """Transfer all messages from a Gmail label to IMAP folder using threaded pipeline."""
        label_id = label['id']
        label_name = label['name']
        folder_name = folder_mapping[label_id]
        
        logging.info(f"Processing label: {label_name} -> {folder_name}")
        
        # Skip if already completed
        if self.progress_manager.is_label_completed(label_id):
            logging.info(f"Label {label_name} already completed, skipping")
            return
        
        # Update current label in progress
        self.progress_manager.progress['current_label'] = label_name
        self.progress_manager.save_progress()
        
        # Get all messages for this label
        message_ids = self.gmail_client.get_messages_by_label(label_id)
        
        if not message_ids:
            logging.info(f"No messages found for label {label_name}")
            return
        
        # Use threaded pipeline for concurrent Gmail fetching and IMAP uploading
        self.transfer_label_threaded(message_ids, label_id, label_name, folder_name)
        
        # Final save for this label
        self.progress_manager.save_progress_batch(force=True)
        
        logging.info(f"Completed label: {label_name}")
    
    def transfer_label_threaded(self, message_ids: List[str], label_id: str, label_name: str, folder_name: str) -> None:
        """Transfer messages using threaded pipeline: Gmail fetch thread + IMAP upload thread."""
        
        # Configuration
        batch_size = self.config['settings'].get('batch_size', 50)
        progress_save_interval = self.config['settings'].get('progress_save_interval', 50)
        
        # Thread communication
        message_queue = queue.Queue(maxsize=100)  # Limit queue size to prevent memory issues
        stop_event = threading.Event()
        transfer_id = f"{label_id}_{int(time.time())}"
        
        # Register this transfer in active threads
        transfer_info = {
            'transfer_id': transfer_id,
            'label_id': label_id,
            'label_name': label_name,
            'stop_event': stop_event,
            'start_time': time.time()
        }
        self.active_threads.append(transfer_info)
        
        # Enhanced statistics with thread tracking
        stats = {
            'fetched': 0,
            'uploaded': 0,
            'errors': 0,
            'skipped': 0,
            'fetch_batches': 0,
            'queue_size': 0,
            'gmail_api_calls': 0,
            'imap_uploads': 0
        }
        stats_lock = threading.Lock()
        
        # Thread lifecycle tracking
        thread_status = {
            'fetcher': {'started': False, 'running': False, 'completed': False, 'error': None},
            'uploader': {'started': False, 'running': False, 'completed': False, 'error': None}
        }
        status_lock = threading.Lock()
        
        # Resource monitoring
        process = psutil.Process()
        initial_memory = process.memory_info().rss / (1024 * 1024)  # MB
        initial_connections = len(process.connections())
        
        logging.info(f"🚀 Starting threaded transfer with {len(message_ids)} messages")
        logging.info(f"💾 Initial memory usage: {initial_memory:.1f} MB")
        logging.info(f"🔗 Initial connections: {initial_connections}")
        
        def gmail_fetcher_thread():
            """Thread that fetches messages from Gmail API and puts them in the queue."""
            thread_name = threading.current_thread().name
            thread_id = threading.get_ident()
            
            try:
                with status_lock:
                    thread_status['fetcher']['started'] = True
                    thread_status['fetcher']['running'] = True
                
                logging.info(f"📥 Gmail fetcher thread started (ID: {thread_id}, Name: {thread_name})")
                logging.info(f"📥 Processing {len(message_ids)} messages in batches of {batch_size}")
                
                for i in range(0, len(message_ids), batch_size):
                    if stop_event.is_set() or self.shutdown_requested:
                        logging.info("📥 Gmail fetcher: shutdown requested, stopping batch processing")
                        break
                        
                    batch = message_ids[i:i + batch_size]
                    
                    # Filter out already transferred messages
                    messages_to_fetch = []
                    for message_id in batch:
                        if self.progress_manager.is_message_transferred(message_id, label_id):
                            with stats_lock:
                                stats['skipped'] += 1
                            continue
                        # Skip if already in cache
                        if message_id not in self.message_cache:
                            messages_to_fetch.append(message_id)
                    
                    # Batch fetch messages from Gmail API
                    if messages_to_fetch:
                        logging.info(f"📥 Fetching batch {stats['fetch_batches'] + 1} of {len(messages_to_fetch)} messages from Gmail")
                        
                        # Track resource usage before API call
                        memory_before = process.memory_info().rss / (1024 * 1024)
                        
                        batch_messages = self.gmail_client.get_messages_batch(messages_to_fetch)
                        
                        with stats_lock:
                            stats['fetch_batches'] += 1
                            stats['gmail_api_calls'] += 1
                        
                        # Track resource usage after API call
                        memory_after = process.memory_info().rss / (1024 * 1024)
                        memory_delta = memory_after - memory_before
                        
                        if memory_delta > 10:  # More than 10MB increase
                            logging.warning(f"💾 Memory usage increased by {memory_delta:.1f} MB during batch fetch")
                        
                        # Process and cache the fetched messages
                        for msg_id, message_data in batch_messages.items():
                            if msg_id not in self.message_cache:
                                # Pre-process message data for caching
                                import base64
                                raw_message = base64.urlsafe_b64decode(message_data['raw'])
                                
                                # Parse email to get metadata
                                email_message = email.message_from_bytes(raw_message)
                                
                                # Extract flags
                                flags = []
                                labels = message_data.get('labelIds', [])
                                if 'UNREAD' not in labels:
                                    flags.append('\\Seen')
                                if 'STARRED' in labels:
                                    flags.append('\\Flagged')
                                
                                # Get message date
                                date_header = email_message.get('Date')
                                msg_time = None
                                if date_header:
                                    try:
                                        from email.utils import parsedate_to_datetime
                                        msg_time = parsedate_to_datetime(date_header)
                                    except:
                                        pass
                                
                                # Cache the processed message
                                self.message_cache[msg_id] = {
                                    'raw_message': raw_message,
                                    'flags': flags,
                                    'msg_time': msg_time
                                }
                                self.cache_misses += 1
                                
                                with stats_lock:
                                    stats['fetched'] += 1
                    
                    # Add all messages in this batch to the upload queue
                    for message_id in batch:
                        if stop_event.is_set():
                            break
                        if not self.progress_manager.is_message_transferred(message_id, label_id):
                            # Put message in queue for IMAP upload
                            message_queue.put((message_id, label_id, folder_name))
                            with stats_lock:
                                stats['queue_size'] = message_queue.qsize()
                
                # Signal end of messages
                message_queue.put(None)  # Sentinel value
                
                with status_lock:
                    thread_status['fetcher']['running'] = False
                    thread_status['fetcher']['completed'] = True
                
                logging.info(f"📥 Gmail fetcher thread completed successfully (batches: {stats['fetch_batches']})")
                
            except Exception as e:
                with status_lock:
                    thread_status['fetcher']['running'] = False
                    thread_status['fetcher']['error'] = str(e)
                
                logging.error(f"❌ Gmail fetcher thread failed: {e}")
                logging.error(f"🧵 Thread ID: {thread_id}, Name: {thread_name}")
                stop_event.set()
                message_queue.put(None)  # Ensure uploader thread exits
        
        def imap_uploader_thread():
            """Thread that takes messages from queue and uploads them to IMAP."""
            thread_name = threading.current_thread().name
            thread_id = threading.get_ident()
            
            try:
                with status_lock:
                    thread_status['uploader']['started'] = True
                    thread_status['uploader']['running'] = True
                
                logging.info(f"📤 IMAP uploader thread started (ID: {thread_id}, Name: {thread_name})")
                messages_processed = 0
                consecutive_timeouts = 0
                
                while True:
                    try:
                        # Check for shutdown request
                        if stop_event.is_set() or self.shutdown_requested:
                            logging.info("📤 IMAP uploader: shutdown requested, stopping upload processing")
                            break
                        
                        # Get message from queue (blocks until available)
                        item = message_queue.get(timeout=30)  # 30 second timeout
                        
                        if item is None:  # Sentinel value - end of messages
                            break
                        
                        message_id, msg_label_id, msg_folder_name = item
                        
                        # Skip if already transferred (double-check)
                        if self.progress_manager.is_message_transferred(message_id, msg_label_id):
                            message_queue.task_done()
                            continue
                        
                        try:
                            # Upload message to IMAP
                            upload_start = time.time()
                            self.transfer_message_from_cache(message_id, msg_label_id, msg_folder_name)
                            upload_time = time.time() - upload_start
                            
                            messages_processed += 1
                            consecutive_timeouts = 0  # Reset timeout counter on success
                            
                            with stats_lock:
                                stats['uploaded'] += 1
                                stats['imap_uploads'] += 1
                                stats['queue_size'] = message_queue.qsize()
                            
                            # Log slow uploads
                            if upload_time > 3.0:
                                logging.warning(f"⚠️ Slow IMAP upload: {upload_time:.2f}s for message {message_id}")
                            
                            # Batch save progress periodically
                            if messages_processed % progress_save_interval == 0:
                                self.progress_manager.save_progress_batch()
                                
                                # Log progress with resource usage
                                current_memory = process.memory_info().rss / (1024 * 1024)
                                memory_delta = current_memory - initial_memory
                                logging.info(f"📤 Uploaded {messages_processed} messages (memory: +{memory_delta:.1f}MB)")
                            
                        except Exception as e:
                            logging.error(f"❌ Failed to upload message {message_id}: {e}")
                            with stats_lock:
                                stats['errors'] += 1
                        
                        message_queue.task_done()
                        
                    except queue.Empty:
                        consecutive_timeouts += 1
                        
                        # Timeout - check if fetcher is still running
                        if stop_event.is_set():
                            break
                        
                        # Log waiting status with diagnostics
                        with status_lock:
                            fetcher_running = thread_status['fetcher']['running']
                        
                        if consecutive_timeouts == 1:  # Only log on first timeout
                            logging.info(f"📤 Waiting for more messages... (fetcher running: {fetcher_running}, queue size: {message_queue.qsize()})")
                        elif consecutive_timeouts >= 10:  # After 5 minutes of timeouts
                            logging.warning(f"⚠️ Extended wait for messages ({consecutive_timeouts * 30}s), fetcher running: {fetcher_running}")
                        
                        continue
                
                with status_lock:
                    thread_status['uploader']['running'] = False
                    thread_status['uploader']['completed'] = True
                
                logging.info(f"📤 IMAP uploader thread completed. Processed {messages_processed} messages")
                
            except Exception as e:
                with status_lock:
                    thread_status['uploader']['running'] = False
                    thread_status['uploader']['error'] = str(e)
                
                logging.error(f"❌ IMAP uploader thread failed: {e}")
                logging.error(f"🧵 Thread ID: {thread_id}, Name: {thread_name}")
                stop_event.set()
        
        # Start threads
        fetcher_thread = threading.Thread(target=gmail_fetcher_thread, name="GmailFetcher")
        uploader_thread = threading.Thread(target=imap_uploader_thread, name="ImapUploader")
        
        # Separate progress bars for each thread
        fetcher_pbar = tqdm(total=len(message_ids), desc="📥 Gmail Fetch", position=0, leave=True)
        uploader_pbar = tqdm(total=len(message_ids), desc="📤 IMAP Upload", position=1, leave=True)
        
        try:
            fetcher_thread.start()
            uploader_thread.start()
            
            # Monitor progress and thread health
            last_fetched = 0
            last_uploaded = 0
            monitoring_cycles = 0
            
            while fetcher_thread.is_alive() or uploader_thread.is_alive() or not message_queue.empty():
                time.sleep(1)  # Update every second
                monitoring_cycles += 1
                
                with stats_lock:
                    current_fetched = stats['fetched']
                    current_uploaded = stats['uploaded'] + stats['skipped']
                    
                    # Update progress bars
                    new_fetches = current_fetched - last_fetched
                    new_uploads = current_uploaded - last_uploaded
                    
                    if new_fetches > 0:
                        fetcher_pbar.update(new_fetches)
                        last_fetched = current_fetched
                    
                    if new_uploads > 0:
                        uploader_pbar.update(new_uploads)
                        last_uploaded = current_uploaded
                    
                    # Update progress bar descriptions with real-time stats
                    fetcher_pbar.set_description(f"📥 Gmail Fetch (batches: {stats['fetch_batches']})")
                    uploader_pbar.set_description(f"📤 IMAP Upload (queue: {stats['queue_size']})")
                
                # Thread health monitoring every 30 seconds
                if monitoring_cycles % 30 == 0:
                    current_memory = process.memory_info().rss / (1024 * 1024)
                    memory_delta = current_memory - initial_memory
                    current_connections = len(process.connections())
                    connection_delta = current_connections - initial_connections
                    
                    with status_lock:
                        fetcher_status = "✅" if thread_status['fetcher']['running'] else "⏸️"
                        uploader_status = "✅" if thread_status['uploader']['running'] else "⏸️"
                    
                    logging.info(f"🔍 Thread Health: Fetcher {fetcher_status} | Uploader {uploader_status}")
                    logging.info(f"📊 Resources: Memory +{memory_delta:.1f}MB | Connections +{connection_delta}")
                
                # Check for user interruption or shutdown
                if stop_event.is_set() or self.shutdown_requested:
                    logging.info("🛑 Shutdown requested, stopping thread monitoring")
                    stop_event.set()  # Ensure both threads know to stop
                    break
            
            # Wait for threads to complete with timeout
            logging.info("🔄 Waiting for threads to complete...")
            try:
                fetcher_thread.join(timeout=10)
                if fetcher_thread.is_alive():
                    logging.warning("⚠️ Gmail fetcher thread did not complete in time")
                
                uploader_thread.join(timeout=10)
                if uploader_thread.is_alive():
                    logging.warning("⚠️ IMAP uploader thread did not complete in time")
                    
            except Exception as e:
                logging.error(f"❌ Error joining threads: {e}")
                stop_event.set()
            
            # Final progress update
            with stats_lock:
                final_fetched = stats['fetched']
                final_uploaded = stats['uploaded'] + stats['skipped']
                
                remaining_fetches = final_fetched - last_fetched
                remaining_uploads = final_uploaded - last_uploaded
                
                if remaining_fetches > 0:
                    fetcher_pbar.update(remaining_fetches)
                if remaining_uploads > 0:
                    uploader_pbar.update(remaining_uploads)
        
        finally:
            # Close progress bars
            fetcher_pbar.close()
            uploader_pbar.close()
            
            # Clean up thread tracking - remove current transfer info
            self.active_threads = [t for t in self.active_threads
                                 if t.get('transfer_id') != transfer_id]
        
        # Report comprehensive threading and resource statistics
        final_memory = process.memory_info().rss / (1024 * 1024)
        final_connections = len(process.connections())
        memory_delta = final_memory - initial_memory
        connection_delta = final_connections - initial_connections
        
        with stats_lock, status_lock:
            logging.info("=== THREADING PERFORMANCE ===")
            logging.info(f"Messages fetched: {stats['fetched']} (batches: {stats['fetch_batches']})")
            logging.info(f"Messages uploaded: {stats['uploaded']} (IMAP calls: {stats['imap_uploads']})")
            logging.info(f"Messages skipped: {stats['skipped']}")
            logging.info(f"Upload errors: {stats['errors']}")
            logging.info(f"Total processed: {stats['uploaded'] + stats['skipped']}")
            
            logging.info("=== THREAD LIFECYCLE ===")
            for thread_name, status in thread_status.items():
                status_icon = "✅" if status['completed'] else "❌" if status['error'] else "⏸️"
                logging.info(f"{thread_name.title()}: {status_icon} Started: {status['started']}, Completed: {status['completed']}")
                if status['error']:
                    logging.error(f"  Error: {status['error']}")
            
            logging.info("=== RESOURCE USAGE ===")
            logging.info(f"Memory usage: {initial_memory:.1f}MB → {final_memory:.1f}MB (Δ{memory_delta:+.1f}MB)")
            logging.info(f"Network connections: {initial_connections} → {final_connections} (Δ{connection_delta:+d})")
            
            # Connection health summary
            if hasattr(self.imap_client, 'connection_errors'):
                logging.info(f"IMAP connection errors: {self.imap_client.connection_errors}")
    
    def run(self) -> None:
        """Run the complete transfer process."""
        try:
            logging.info("Starting Gmail to IMAP transfer")
            
            # Setup clients
            self.setup_clients()
            
            # Get Gmail labels
            labels = self.gmail_client.get_labels()
            
            # Filter out system labels that shouldn't be transferred
            system_labels = ['CHAT', 'CATEGORY_FORUMS', 'CATEGORY_UPDATES', 'CATEGORY_PROMOTIONS', 'CATEGORY_SOCIAL']
            labels = [label for label in labels if label['id'] not in system_labels]
            
            # Create folder mappings
            folder_mapping = self.create_folder_mappings(labels)
            
            # Update progress with total labels
            self.progress_manager.progress['total_labels'] = len(labels)
            self.progress_manager.save_progress()
            
            # Transfer each label
            for label in labels:
                self.transfer_label(label, folder_mapping)
                
                # Update completed labels count
                self.progress_manager.progress['completed_labels'] += 1
                self.progress_manager.save_progress()
            
            # Report cache statistics
            self.report_cache_statistics()
            logging.info("Transfer completed successfully")
            
        except Exception as e:
            logging.error(f"Transfer failed: {e}")
            raise
        finally:
            # Cleanup
            if self.imap_client:
                self.imap_client.disconnect()
    
    def report_cache_statistics(self) -> None:
        """Report message cache and batch request performance statistics."""
        total_requests = self.cache_hits + self.cache_misses
        if total_requests > 0:
            hit_rate = (self.cache_hits / total_requests) * 100
            logging.info("=== PERFORMANCE STATISTICS ===")
            logging.info(f"Cache hits: {self.cache_hits}")
            logging.info(f"Cache misses: {self.cache_misses}")
            logging.info(f"Total message requests: {total_requests}")
            logging.info(f"Cache hit rate: {hit_rate:.1f}%")
            logging.info(f"Duplicate messages avoided: {self.cache_hits}")
            
            # Estimate API call reduction from batching
            gmail_batch_size = self.config['settings'].get('gmail_batch_size', 50)
            estimated_individual_calls = self.cache_misses
            estimated_batch_calls = (self.cache_misses + gmail_batch_size - 1) // gmail_batch_size
            api_call_reduction = estimated_individual_calls - estimated_batch_calls
            
            logging.info(f"Gmail API calls without batching: ~{estimated_individual_calls}")
            logging.info(f"Gmail API calls with batching: ~{estimated_batch_calls}")
            logging.info(f"API calls saved by batching: ~{api_call_reduction}")
            
            if self.cache_hits > 0:
                logging.info("✅ Message deduplication is working - avoiding redundant Gmail API calls")
            if api_call_reduction > 0:
                logging.info("✅ Gmail API batching is working - reducing HTTP requests significantly")
            if self.cache_hits == 0 and api_call_reduction == 0:
                logging.info("ℹ️ No performance optimizations triggered in this transfer")


def verify_label_coverage(transfer: 'GmailToImapTransfer') -> bool:
    """Verify that all Gmail labels are properly mapped and will be synced."""
    try:
        # Get all Gmail labels
        labels = transfer.gmail_client.get_labels()
        
        # Filter out system labels (same as in main transfer)
        system_labels = ['CHAT', 'CATEGORY_FORUMS', 'CATEGORY_UPDATES', 'CATEGORY_PROMOTIONS', 'CATEGORY_SOCIAL']
        filtered_labels = [label for label in labels if label['id'] not in system_labels]
        
        # Create folder mappings to test
        folder_mapping = transfer.create_folder_mappings(filtered_labels)
        
        # Verify coverage
        all_labels_mapped = len(folder_mapping) == len(filtered_labels)
        
        logging.info("=== LABEL COVERAGE VERIFICATION ===")
        logging.info(f"Total Gmail labels found: {len(labels)}")
        logging.info(f"System labels filtered out: {len(labels) - len(filtered_labels)}")
        logging.info(f"Labels to be synced: {len(filtered_labels)}")
        logging.info(f"Folder mappings created: {len(folder_mapping)}")
        logging.info(f"Coverage complete: {all_labels_mapped}")
        
        if not all_labels_mapped:
            logging.error("❌ Not all labels are mapped!")
            return False
        else:
            logging.info("✅ All Gmail labels will be synced (configured + dynamic)")
            return True
            
    except Exception as e:
        logging.error(f"Label coverage verification failed: {e}")
        return False


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Transfer emails from Gmail to IMAP server')
    parser.add_argument('--config', default='config.yaml', help='Configuration file path')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose logging')
    parser.add_argument('--verify-labels', action='store_true', help='Only verify label mapping coverage without transferring')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be transferred without doing it')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        transfer = GmailToImapTransfer(args.config)
        
        # Setup clients for verification
        transfer.setup_clients()
        
        if args.verify_labels:
            # Only verify label coverage
            success = verify_label_coverage(transfer)
            return 0 if success else 1
        elif args.dry_run:
            # Show what would be done
            logging.info("=== DRY RUN MODE ===")
            verify_label_coverage(transfer)
            
            # Get Gmail labels and show transfer plan
            labels = transfer.gmail_client.get_labels()
            system_labels = ['CHAT', 'CATEGORY_FORUMS', 'CATEGORY_UPDATES', 'CATEGORY_PROMOTIONS', 'CATEGORY_SOCIAL']
            filtered_labels = [label for label in labels if label['id'] not in system_labels]
            
            logging.info("=== TRANSFER PLAN ===")
            for label in filtered_labels:
                message_count = len(transfer.gmail_client.get_messages_by_label(label['id']))
                logging.info(f"Label '{label['name']}': {message_count} messages")
            
            logging.info("=== DRY RUN COMPLETE ===")
            return 0
        else:
            # Normal transfer
            transfer.run()
            
    except KeyboardInterrupt:
        logging.info("Transfer interrupted by user")
    except Exception as e:
        logging.error(f"Transfer failed: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())