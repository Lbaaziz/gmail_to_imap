#!/usr/bin/env python3
"""
Utility functions for Gmail to IMAP transfer system.
"""

import time
import logging


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