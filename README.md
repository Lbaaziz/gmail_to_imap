# Gmail to IMAP Transfer System

A Python-based system that transfers emails from Gmail to any IMAP server while preserving folder structure, metadata, and content. Features progress tracking and resumable transfers.

## Features

- **OAuth 2.0 Authentication**: Secure Gmail access without storing passwords
- **Progress Tracking**: Resume interrupted transfers from last checkpoint
- **Folder Mapping**: Automatically maps Gmail labels to IMAP folders
- **Metadata Preservation**: Preserves read/unread status, flags, and timestamps
- **Duplicate Detection**: Skips already transferred emails
- **Error Recovery**: Automatic retry mechanism with exponential backoff
- **Progress Reporting**: Real-time progress bars and detailed logging

## Requirements

- Python 3.8 or higher
- Gmail account with API access enabled
- Target IMAP server credentials

## Installation

1. **Clone or download this project**
   ```bash
   git clone <repository-url>
   cd gmail_to_imap
   ```

2. **Create a virtual environment (recommended)**
   ```bash
   python -m venv gmail_to_imap_env
   source gmail_to_imap_env/bin/activate  # On Windows: gmail_to_imap_env\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Gmail API Setup

### 1. Enable Gmail API
1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Enable the Gmail API:
   - Go to "APIs & Services" > "Library"
   - Search for "Gmail API" and enable it

### 2. Create OAuth 2.0 Credentials
1. Go to "APIs & Services" > "Credentials"
2. Click "Create Credentials" > "OAuth client ID"
3. Choose "Web application"
4. In redirect Url put http://localhost:8085
5. Download the credentials JSON file
6. Rename it to `credentials.json` and place it in the project directory

### 3. Configure OAuth Consent Screen (if needed)
1. Go to "APIs & Services" > "OAuth consent screen"
2. Choose "External" (unless you have Google Workspace)
3. Fill in the required fields (app name, user support email, etc.)
4. Add your email to test users in development mode

## Configuration

1. **Copy and edit the configuration file**
   ```bash
   cp config_sample.yaml config.yaml
   ```

2. **Edit `config.yaml` with your settings**
   ```yaml
   gmail:
     credentials_file: "credentials.json"
     
   imap:
     server: "your-imap-server.com"  # e.g., "imap.example.com"
     port: 993                       # Usually 993 for SSL, 143 for non-SSL
     username: "your-email@domain.com"
     password: "your-imap-password"
     use_ssl: true
   
   settings:
     batch_size: 50                  # Adjust based on server limits
     max_retries: 3
     resume_from_progress: true
     
     # Customize label mappings as needed
     label_mappings:
       "INBOX": "INBOX"
       "SENT": "Sent"
       "DRAFT": "Drafts"
       "SPAM": "Junk"
       "TRASH": "Trash"
   ```

## Usage

### Basic Usage
```bash
python gmail_to_imap.py
```

### With Custom Configuration
```bash
python gmail_to_imap.py --config my_config.yaml
```

### Verbose Mode
```bash
python gmail_to_imap.py --verbose
```

### Command Line Options
- `--config`: Specify custom configuration file (default: `config.yaml`)
- `--verbose`, `-v`: Enable verbose logging
- `--help`, `-h`: Show help message

## First Run

1. **Prepare your configuration**
   - Ensure `credentials.json` is in the project directory
   - Update `config.yaml` with your IMAP server details

2. **Run the transfer**
   ```bash
   python gmail_to_imap.py
   ```

3. **Complete OAuth flow**
   - A browser window will open for Gmail authentication
   - Grant the necessary permissions
   - The browser will show a success message
   - Return to the terminal to see the transfer progress

4. **Monitor progress**
   - Progress is displayed with real-time progress bars
   - Detailed logs are written to `gmail_to_imap.log`
   - Progress is saved to `progress.json` for resumability

## File Structure

```
gmail_to_imap/
├── gmail_to_imap.py          # Main application
├── config.yaml               # Configuration template
├── credentials.json          # Gmail OAuth credentials (you provide)
├── token.json               # OAuth token (auto-generated)
├── progress.json            # Progress tracking (auto-generated)
├── gmail_to_imap.log        # Application logs (auto-generated)
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Progress Tracking and Resumability

The system automatically tracks progress in `progress.json`. If the transfer is interrupted:

1. **Automatic Resume**: Simply run the command again
2. **Skip Completed**: Already transferred emails are automatically skipped
3. **Progress Display**: Shows completion status for each label/folder

### Progress File Structure
```json
{
  "session_id": "2024-01-15_12-30-45",
  "total_labels": 5,
  "completed_labels": 2,
  "current_label": "INBOX",
  "transferred_messages": {
    "INBOX": ["message_id_1", "message_id_2"],
    "SENT": ["message_id_3"]
  },
  "label_folder_mapping": {
    "INBOX": "INBOX",
    "SENT": "Sent"
  }
}
```

## Configuration Options

### Gmail Settings
- `credentials_file`: Path to Gmail OAuth credentials JSON file

### IMAP Settings
- `server`: IMAP server hostname
- `port`: IMAP server port (993 for SSL, 143 for non-SSL)
- `username`: IMAP username/email
- `password`: IMAP password
- `use_ssl`: Enable SSL/TLS encryption (recommended: `true`)

### Transfer Settings
- `batch_size`: Number of emails to process per batch (default: 50)
- `max_retries`: Maximum retry attempts for failed operations (default: 3)
- `resume_from_progress`: Enable resumable transfers (default: `true`)
- `label_mappings`: Custom mapping from Gmail labels to IMAP folder names

## Troubleshooting

### Common Issues

1. **Authentication Error**
   ```
   Error: Gmail credentials file 'credentials.json' not found
   ```
   **Solution**: Download OAuth credentials from Google Cloud Console

2. **IMAP Connection Failed**
   ```
   Error: Failed to connect to IMAP server
   ```
   **Solutions**:
   - Verify server hostname and port
   - Check username and password
   - Ensure IMAP is enabled on the target server
   - Check firewall settings

3. **Gmail API Quota Exceeded**
   ```
   Error: Quota exceeded for quota metric 'Queries' and limit 'Queries per day'
   ```
   **Solution**: Wait for quota reset (usually 24 hours) or request quota increase

4. **SSL Certificate Error**
   ```
   Error: SSL certificate verification failed
   ```
   **Solution**: Check server SSL configuration or temporarily set `use_ssl: false` for testing

### Debug Mode

Enable verbose logging for detailed troubleshooting:
```bash
python gmail_to_imap.py --verbose
```

### Log Files

Check `gmail_to_imap.log` for detailed operation logs:
```bash
tail -f gmail_to_imap.log
```

## Performance Tips

1. **Adjust Batch Size**: Increase `batch_size` for faster transfers (but watch for memory usage)
2. **Network Stability**: Ensure stable internet connection for large transfers
3. **IMAP Server Limits**: Some servers have rate limits; adjust `batch_size` accordingly
4. **Resume Strategy**: For very large transfers, run in smaller chunks and resume as needed

## Security Considerations

1. **Credential Storage**: Keep `credentials.json` and `config.yaml` secure
2. **OAuth Tokens**: `token.json` contains sensitive data - don't share it
3. **IMAP Passwords**: Consider using app-specific passwords where available
4. **Network Security**: Always use SSL/TLS for IMAP connections

## Limitations

1. **Gmail API Limits**: Subject to Google's API quotas and rate limits
2. **IMAP Server Limits**: Performance depends on target IMAP server capabilities
3. **Large Attachments**: Very large emails might require special handling
4. **Special Characters**: Some folder names with special characters might need manual mapping

## Support

For issues and questions:
1. Check this README and troubleshooting section
2. Review the log files for error details
3. Verify your configuration settings
4. Test with a small subset of emails first

## License

This project is provided as-is for educational and personal use. Please ensure compliance with Gmail's Terms of Service and your IMAP provider's policies.