#!/usr/bin/env python3
"""
Gmail to IMAP Transfer System

A simplified Python-based system that transfers emails from Gmail to any IMAP server
while preserving folder structure, metadata, and content. Supports progress tracking
and resumable transfers.
"""

import logging

# Local imports
from transfer_orchestrator import GmailToImapTransfer, verify_label_coverage


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