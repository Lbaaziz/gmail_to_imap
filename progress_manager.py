#!/usr/bin/env python3
"""
Progress tracking and resumable transfers for Gmail to IMAP transfer system.
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, Any


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