#!/usr/bin/env python3
"""
IMAP client for Gmail to IMAP transfer system.
"""

import time
import logging
from datetime import datetime
from typing import List

# IMAP imports
import imapclient


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