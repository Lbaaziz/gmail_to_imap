#!/usr/bin/env python3
"""
Setup script for Gmail to IMAP Transfer System
Helps users get started quickly with the correct environment setup.
"""

import os
import sys
import subprocess
import argparse

def run_command(command, description=""):
    """Run a shell command and handle errors."""
    if description:
        print(f"→ {description}")
    
    try:
        result = subprocess.run(command, shell=True, check=True, 
                              capture_output=True, text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        print(f"Output: {e.output}")
        return None

def setup_virtual_environment():
    """Create and activate virtual environment."""
    venv_name = "gmail_to_imap_env"
    
    if os.path.exists(venv_name):
        print(f"✓ Virtual environment '{venv_name}' already exists")
        return True
    
    print(f"Creating virtual environment '{venv_name}'...")
    result = run_command(f"python -m venv {venv_name}", 
                        "Creating virtual environment")
    
    if result is None:
        print("Failed to create virtual environment")
        return False
    
    print(f"✓ Virtual environment '{venv_name}' created successfully")
    return True

def install_dependencies():
    """Install required Python packages."""
    print("Installing dependencies...")
    
    # Check if we're in a virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        pip_command = "pip"
    else:
        # Try to use virtual environment pip if it exists
        venv_pip = os.path.join("gmail_to_imap_env", "bin", "pip")
        if os.path.exists(venv_pip):
            pip_command = venv_pip
        else:
            pip_command = "pip"
    
    result = run_command(f"{pip_command} install -r requirements.txt", 
                        "Installing Python packages")
    
    if result is None:
        print("Failed to install dependencies")
        return False
    
    print("✓ Dependencies installed successfully")
    return True

def create_example_credentials():
    """Create example credentials file."""
    if os.path.exists("credentials.json"):
        print("✓ credentials.json already exists")
        return True
    
    example_credentials = """{
  "installed": {
    "client_id": "your-client-id.apps.googleusercontent.com",
    "project_id": "your-project-id",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_secret": "your-client-secret",
    "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"]
  }
}"""
    
    with open("credentials_example.json", "w") as f:
        f.write(example_credentials)
    
    print("✓ Created credentials_example.json")
    print("  → Download your actual credentials.json from Google Cloud Console")
    return True

def run_tests():
    """Run setup tests."""
    print("Running setup tests...")
    result = run_command("python test_setup.py", "Testing system setup")
    
    if result is None:
        print("Setup tests failed")
        return False
    
    print("✓ Setup tests passed")
    return True

def print_next_steps():
    """Print instructions for next steps."""
    print("\n" + "=" * 60)
    print("🎉 SETUP COMPLETE!")
    print("=" * 60)
    print("\nNext steps to start using the system:")
    print("\n1. GET GMAIL CREDENTIALS:")
    print("   • Go to: https://console.cloud.google.com/")
    print("   • Create a new project or select existing one")
    print("   • Enable Gmail API")
    print("   • Create OAuth 2.0 credentials (Desktop Application)")
    print("   • Download credentials.json file")
    print("   • Place credentials.json in this directory")
    
    print("\n2. CONFIGURE IMAP SETTINGS:")
    print("   • Edit config.yaml with your IMAP server details")
    print("   • Update server, username, and password")
    
    print("\n3. RUN THE TRANSFER:")
    if os.path.exists("gmail_to_imap_env"):
        print("   • Activate virtual environment:")
        if os.name == 'nt':  # Windows
            print("     gmail_to_imap_env\\Scripts\\activate")
        else:  # Unix/Linux/Mac
            print("     source gmail_to_imap_env/bin/activate")
    print("   • Run: python gmail_to_imap.py")
    
    print("\n4. OPTIONAL - TEST FIRST:")
    print("   • Run: python test_setup.py")
    print("   • This validates your setup before transfer")
    
    print(f"\nFor detailed instructions, see: README.md")

def main():
    """Main setup function."""
    parser = argparse.ArgumentParser(description='Setup Gmail to IMAP Transfer System')
    parser.add_argument('--skip-venv', action='store_true', 
                       help='Skip virtual environment creation')
    parser.add_argument('--skip-deps', action='store_true', 
                       help='Skip dependency installation')
    parser.add_argument('--skip-tests', action='store_true', 
                       help='Skip running tests')
    
    args = parser.parse_args()
    
    print("Gmail to IMAP Transfer System - Setup")
    print("=" * 40)
    
    success = True
    
    # Create virtual environment
    if not args.skip_venv:
        if not setup_virtual_environment():
            success = False
    
    # Install dependencies
    if not args.skip_deps and success:
        if not install_dependencies():
            success = False
    
    # Create example files
    if success:
        create_example_credentials()
    
    # Run tests
    if not args.skip_tests and success:
        if not run_tests():
            success = False
    
    # Print results
    if success:
        print_next_steps()
        return 0
    else:
        print("\n❌ Setup failed. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())