#!/usr/bin/env python3
"""
Main transfer orchestrator for Gmail to IMAP transfer system.
"""

import os
import time
import email
import base64
import logging
import threading
import queue
import signal
from datetime import datetime
from typing import Dict, List, Optional, Any
import psutil
from email.utils import parsedate_to_datetime

# Progress bar
from tqdm import tqdm

# Local imports
from config_manager import ConfigManager
from progress_manager import ProgressManager
from gmail_client import GmailClient
from imap_client import IMAPClient
from utils import safe_transfer


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
        
        # Progressive cache cleanup - remove message from cache after successful upload
        self._cleanup_message_from_cache(message_id)
    
    def _cleanup_message_from_cache(self, message_id: str) -> None:
        """Remove a message from cache after successful upload to free memory."""
        if message_id in self.message_cache:
            # Calculate memory being freed (rough estimate)
            cached_data = self.message_cache[message_id]
            message_size = len(cached_data.get('raw_message', b''))
            del self.message_cache[message_id]
            
            # Log cache cleanup periodically
            if not hasattr(self, '_cache_cleanups'):
                self._cache_cleanups = 0
            self._cache_cleanups += 1
            
            # Log every 100 cleanups to avoid spam
            if self._cache_cleanups % 100 == 0:
                current_cache_size = len(self.message_cache)
                logging.info(f"💾 Cache cleanup: {self._cache_cleanups} messages freed, {current_cache_size} remaining in cache")
    
    def _monitor_cache_memory(self) -> None:
        """Monitor and report cache memory usage."""
        if not self.message_cache:
            return
            
        # Calculate approximate cache memory usage
        total_message_size = 0
        for cached_data in self.message_cache.values():
            total_message_size += len(cached_data.get('raw_message', b''))
        
        cache_size_mb = total_message_size / (1024 * 1024)
        message_count = len(self.message_cache)
        
        # Log if cache is getting large
        if cache_size_mb > 100:  # More than 100MB
            logging.warning(f"💾 Large cache detected: {message_count} messages, ~{cache_size_mb:.1f}MB")
        elif message_count > 1000:  # More than 1000 messages
            logging.info(f"💾 Cache status: {message_count} messages, ~{cache_size_mb:.1f}MB")
    
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
                                
                                # Log progress with resource usage and cache monitoring
                                current_memory = process.memory_info().rss / (1024 * 1024)
                                memory_delta = current_memory - initial_memory
                                cache_size = len(self.message_cache)
                                logging.info(f"📤 Uploaded {messages_processed} messages (memory: +{memory_delta:.1f}MB, cache: {cache_size} messages)")
                                
                                # Monitor cache memory usage
                                self._monitor_cache_memory()
                            
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
            
            # Cache cleanup summary
            final_cache_size = len(self.message_cache)
            cache_cleanups = getattr(self, '_cache_cleanups', 0)
            logging.info(f"Cache cleanups: {cache_cleanups} messages removed from cache")
            logging.info(f"Final cache size: {final_cache_size} messages")
            
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
            
            # Clear any remaining cache to free memory
            if hasattr(self, 'message_cache'):
                cache_size = len(self.message_cache)
                if cache_size > 0:
                    logging.info(f"💾 Clearing remaining cache: {cache_size} messages")
                    self.message_cache.clear()
    
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