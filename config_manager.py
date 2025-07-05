#!/usr/bin/env python3
"""
Configuration management for Gmail to IMAP transfer system.
"""

import yaml
from typing import Dict, Any


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