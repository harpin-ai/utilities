#!/usr/bin/env python3
"""
CSV File Upload Script for harpin AI

Uploads CSV files to harpin AI following the documented 6-step process.
Suitable for automated ETL pipelines and cron jobs.

REQUIRES: Python 3.7 or higher

Usage:
    python3 upload_to_harpin.py <sourceId> <fileName>

Environment Variables Required:
    HARPIN_CLIENT_ID - Client ID for authentication
    HARPIN_REFRESH_TOKEN - Refresh token for authentication
"""

# Check Python version before any imports
import sys
if sys.version_info < (3, 7):
    print("Error: This script requires Python 3.7 or higher.", file=sys.stderr)
    print(f"You are using Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}", file=sys.stderr)
    print("\nPlease upgrade Python or use 'python3' command instead of 'python'.", file=sys.stderr)
    sys.exit(1)

import os
import time
import json
import argparse
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from datetime import datetime

try:
    import requests
except ImportError:
    print("Error: 'requests' library is required. Install with: pip3 install requests", file=sys.stderr)
    print("  (or: python3 -m pip install requests)", file=sys.stderr)
    sys.exit(2)

try:
    from tqdm import tqdm
except ImportError:
    print("Error: 'tqdm' library is required. Install with: pip3 install tqdm", file=sys.stderr)
    print("  (or: python3 -m pip install tqdm)", file=sys.stderr)
    sys.exit(2)

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

API_BASE_URL = "https://api.harpin.ai"
MAX_CONCURRENT_UPLOADS = 3  # Maximum number of concurrent uploads allowed
MAX_FILE_SIZE_GB = 5  # Maximum file size in GB
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_GB * 1024 * 1024 * 1024
RETRY_ATTEMPTS = 3  # Number of retry attempts for network failures
RETRY_DELAY_SECONDS = 10  # Delay between retries
POLL_INTERVAL_SECONDS = 5  # Polling interval for status checks

# Exit codes
EXIT_SUCCESS = 0
EXIT_USER_ERROR = 1
EXIT_SYSTEM_ERROR = 2

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def log_info(message: str):
    """Print info message to stdout."""
    print(f"ℹ {message}")

def log_success(message: str):
    """Print success message to stdout."""
    print(f"✓ {message}")

def log_error(message: str):
    """Print error message to stderr."""
    print(f"✗ {message}", file=sys.stderr)

def log_progress(message: str):
    """Print progress message to stdout."""
    print(f"⏳ {message}")

def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def retry_on_network_error(func):
    """Decorator to retry function on network errors."""
    def wrapper(*args, **kwargs):
        last_exception = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                last_exception = e
                if attempt < RETRY_ATTEMPTS:
                    log_progress(f"Network error (attempt {attempt}/{RETRY_ATTEMPTS}): {str(e)}")
                    log_progress(f"Retrying in {RETRY_DELAY_SECONDS} seconds...")
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    log_error(f"Network error after {RETRY_ATTEMPTS} attempts: {str(e)}")
        raise last_exception
    return wrapper

# ============================================================================
# AUTHENTICATION
# ============================================================================

def get_access_token() -> str:
    """
    Obtain access token from harpin AI using environment variables.
    
    Returns:
        Access token string
        
    Exits with code 1 if:
        - Environment variables are missing
        - Authentication fails
    """
    client_id = os.environ.get('HARPIN_CLIENT_ID', '').strip()
    refresh_token = os.environ.get('HARPIN_REFRESH_TOKEN', '').strip()
    
    missing_vars = []
    if not client_id:
        missing_vars.append('HARPIN_CLIENT_ID')
    if not refresh_token:
        missing_vars.append('HARPIN_REFRESH_TOKEN')
    
    if missing_vars:
        log_error("Missing required environment variables:")
        for var in missing_vars:
            log_error(f"  - {var}")
        log_error("\nPlease set these environment variables before running the script.")
        sys.exit(EXIT_USER_ERROR)
    
    log_progress("Authenticating with harpin AI...")

    try:
        response = requests.post(
            f"{API_BASE_URL}/token",
            json={
                "clientId": client_id,
                "refreshToken": refresh_token
            },
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            access_token = data.get('accessToken')
            if access_token:
                log_success("Authentication successful")
                return access_token
            else:
                log_error("Authentication response missing access token")
                sys.exit(EXIT_USER_ERROR)
        else:
            log_error(f"Authentication failed with status {response.status_code}")
            try:
                error_data = response.json()
                log_error(f"Error details: {json.dumps(error_data, indent=2)}")
            except:
                log_error(f"Response: {response.text}")
            sys.exit(EXIT_USER_ERROR)
            
    except requests.exceptions.RequestException as e:
        log_error(f"Authentication request failed: {str(e)}")
        sys.exit(EXIT_USER_ERROR)

# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

def validate_file(file_path: str) -> Tuple[Path, int]:
    """
    Validate that file exists, is readable, and within size limits.
    
    Args:
        file_path: Path to the CSV file
        
    Returns:
        Tuple of (Path object, file size in bytes)
        
    Exits with code 1 if validation fails.
    """
    log_progress("Validating file...")
    
    path = Path(file_path)
    
    # Check if file exists
    if not path.exists():
        log_error(f"File not found: {file_path}")
        sys.exit(EXIT_USER_ERROR)
    
    # Check if it's a file (not a directory)
    if not path.is_file():
        log_error(f"Path is not a file: {file_path}")
        sys.exit(EXIT_USER_ERROR)
    
    # Check if file is readable
    if not os.access(path, os.R_OK):
        log_error(f"File is not readable: {file_path}")
        sys.exit(EXIT_USER_ERROR)
    
    # Check file size
    file_size = path.stat().st_size
    if file_size > MAX_FILE_SIZE_BYTES:
        log_error(f"File size ({format_file_size(file_size)}) exceeds maximum allowed size ({MAX_FILE_SIZE_GB} GB)")
        sys.exit(EXIT_USER_ERROR)
    
    log_success(f"File validated: {path.name} ({format_file_size(file_size)})")
    return path, file_size

def validate_source(source_id: str, access_token: str) -> bool:
    """
    Validate that source ID exists.
    
    Args:
        source_id: The source ID to validate
        access_token: Bearer token for authentication
        
    Returns:
        True if source exists
        
    Exits with code 1 if source is invalid (after showing available sources).
    """
    log_progress(f"Validating source ID: {source_id}...")
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        # Try to get the specific source
        response = requests.get(
            f"{API_BASE_URL}/sources/{source_id}",
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            source_data = response.json()
            source_system = source_data.get('sourceSystem', 'unknown')
            
            # Check if sourceSystem is flatFile
            if source_system != 'flatFile':
                log_error(f"Invalid source type: sourceSystem is of type '{source_system}', must be of type 'flatFile'")
                sys.exit(EXIT_USER_ERROR)
            
            log_success(f"Source ID validated: {source_id}")
            return True
        elif response.status_code == 404:
            log_error(f"Invalid source ID: {source_id}")
            log_info("Fetching available sources...")
            
            # Get list of available sources
            try:
                sources_response = requests.get(
                    f"{API_BASE_URL}/sources",
                    headers=headers,
                    timeout=30
                )
                
                if sources_response.status_code == 200:
                    sources_data = sources_response.json()
                    sources = sources_data.get('content', [])
                    
                    # Filter to only show flatFile sources
                    flat_file_sources = [s for s in sources if s.get('sourceSystem') == 'flatFile']
                    
                    if flat_file_sources:
                        log_info("\nAvailable flatFile sources:")
                        for source in flat_file_sources:
                            source_id_val = source.get('id', 'N/A')
                            source_name = source.get('name', 'N/A')
                            log_info(f"ID: {source_id_val}, Name: {source_name}")
                    else:
                        log_info("No flatFile sources available")
                else:
                    log_error("Failed to retrieve available sources")
            except requests.exceptions.RequestException as e:
                log_error(f"Failed to retrieve available sources: {str(e)}")
            
            sys.exit(EXIT_USER_ERROR)
        else:
            log_error(f"Failed to validate source ID (status {response.status_code})")
            sys.exit(EXIT_USER_ERROR)
            
    except requests.exceptions.RequestException as e:
        log_error(f"Failed to validate source ID: {str(e)}")
        sys.exit(EXIT_USER_ERROR)

def check_concurrent_uploads(source_id: str, access_token: str):
    """
    Check if concurrent upload limit has been reached.
    
    Args:
        source_id: The source ID
        access_token: Bearer token for authentication
        
    Exits with code 1 if limit is reached.
    """
    log_progress("Checking concurrent uploads...")
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(
            f"{API_BASE_URL}/sources/{source_id}/uploads",
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            uploads = response.json()
            
            # Handle paginated/wrapped response (dict with 'content' field) or direct list
            if isinstance(uploads, dict):
                uploads = uploads.get('content', [])
            elif not isinstance(uploads, list):
                log_error(f"Unexpected response format from uploads API: expected list or dict, got {type(uploads).__name__}")
                sys.exit(EXIT_SYSTEM_ERROR)
            
            # Count in-progress uploads
            in_progress_statuses = ['created', 'analysisInProgress', 'analysisCompleted', 'importRequested', 'importInProgress']
            in_progress = []
            
            for u in uploads:
                # Skip non-dictionary items with a warning
                if not isinstance(u, dict):
                    log_info(f"Warning: Skipping non-dictionary upload item: {type(u).__name__}")
                    continue
                
                status = u.get('status')
                if status in in_progress_statuses:
                    in_progress.append(u)
            
            in_progress_count = len(in_progress)
            
            if in_progress_count >= MAX_CONCURRENT_UPLOADS:
                log_error(f"Maximum concurrent uploads ({MAX_CONCURRENT_UPLOADS}) reached")
                log_error(f"Currently {in_progress_count} upload(s) in progress")
                log_info("Please wait for existing uploads to complete before starting a new one")
                sys.exit(EXIT_USER_ERROR)
            
            log_success(f"Concurrent uploads check passed ({in_progress_count}/{MAX_CONCURRENT_UPLOADS})")
        else:
            log_error(f"Failed to check concurrent uploads (status {response.status_code})")
            sys.exit(EXIT_SYSTEM_ERROR)
            
    except requests.exceptions.RequestException as e:
        log_error(f"Failed to check concurrent uploads: {str(e)}")
        sys.exit(EXIT_SYSTEM_ERROR)

# ============================================================================
# UPLOAD WORKFLOW
# ============================================================================

@retry_on_network_error
def create_upload(source_id: str, file_name: str, access_token: str) -> Tuple[str, str]:
    """
    Create an upload and get presigned URL.
    
    Args:
        source_id: The source ID
        file_name: Name of the file to upload
        access_token: Bearer token for authentication
        
    Returns:
        Tuple of (upload_id, presigned_url)
    """
    log_progress("Creating upload...")
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(
        f"{API_BASE_URL}/sources/{source_id}/uploads",
        json={"fileName": file_name},
        headers=headers,
        timeout=30
    )
    
    if response.status_code in [200, 201]:
        data = response.json()
        upload_id = data.get('id')
        presigned_url = data.get('url')
        
        if not upload_id or not presigned_url:
            log_error("Create upload response missing required fields")
            sys.exit(EXIT_SYSTEM_ERROR)
        
        log_success(f"Upload created: {upload_id}")
        return upload_id, presigned_url
    else:
        log_error(f"Failed to create upload (status {response.status_code})")
        try:
            error_data = response.json()
            log_error(f"Error details: {json.dumps(error_data, indent=2)}")
        except:
            log_error(f"Response: {response.text}")
        sys.exit(EXIT_SYSTEM_ERROR)

class ProgressFileWrapper:
    """
    File-like wrapper that updates a progress bar as data is read.
    This avoids chunked transfer encoding while maintaining streaming capability.
    """
    def __init__(self, file_obj, progress_bar):
        self.file_obj = file_obj
        self.progress_bar = progress_bar
    
    def read(self, size=-1):
        """Read data and update progress bar."""
        data = self.file_obj.read(size)
        if data:
            self.progress_bar.update(len(data))
        return data
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        pass

@retry_on_network_error
def upload_to_s3(presigned_url: str, file_path: Path, file_size: int):
    """
    Upload file to S3 using presigned URL with progress indicator.
    
    Args:
        presigned_url: The presigned S3 URL
        file_path: Path to the file to upload
        file_size: Size of the file in bytes
    """
    log_progress(f"Uploading {file_path.name} to S3...")
    
    # Use tqdm for progress bar with custom file wrapper
    with open(file_path, 'rb') as f:
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=file_path.name) as pbar:
            wrapped_file = ProgressFileWrapper(f, pbar)
            
            response = requests.put(
                presigned_url,
                data=wrapped_file,
                headers={
                    "Content-Type": "text/csv",
                    "Content-Length": str(file_size)
                },
                timeout=300
            )
    
    if response.status_code == 200:
        log_success("File uploaded to S3 successfully")
    else:
        log_error(f"S3 upload failed (status {response.status_code})")
        log_error(f"Response: {response.text}")
        sys.exit(EXIT_SYSTEM_ERROR)

@retry_on_network_error
def poll_status(source_id: str, upload_id: str, access_token: str, target_status: str, phase_name: str) -> Dict[str, Any]:
    """
    Poll upload status until target status is reached.
    
    Args:
        source_id: The source ID
        upload_id: The upload ID
        access_token: Bearer token for authentication
        target_status: The status to wait for
        phase_name: Name of the phase for logging
        
    Returns:
        Final status response data
    """
    log_progress(f"Waiting for {phase_name}...")
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    start_time = time.time()
    last_status = None
    
    while True:
        response = requests.get(
            f"{API_BASE_URL}/sources/{source_id}/uploads/{upload_id}",
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            current_status = data.get('status')
            
            # Log status changes
            if current_status != last_status:
                log_info(f"Status: {current_status}")
                last_status = current_status
            
            # Check for failure
            if current_status == 'failed':
                error_message = data.get('errorMessage', 'Unknown error')
                log_error(f"{phase_name} failed: {error_message}")
                sys.exit(EXIT_SYSTEM_ERROR)
            
            # Check if target status reached
            if current_status == target_status:
                elapsed = time.time() - start_time
                log_success(f"{phase_name} completed in {elapsed:.1f} seconds")
                return data
            
            # Wait before next poll
            time.sleep(POLL_INTERVAL_SECONDS)
        else:
            log_error(f"Failed to poll status (status {response.status_code})")
            sys.exit(EXIT_SYSTEM_ERROR)

@retry_on_network_error
def request_import(source_id: str, upload_id: str, access_token: str):
    """
    Request import after analysis is complete.
    
    Args:
        source_id: The source ID
        upload_id: The upload ID
        access_token: Bearer token for authentication
    """
    log_progress("Requesting import...")
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    response = requests.put(
        f"{API_BASE_URL}/sources/{source_id}/uploads/{upload_id}/status",
        json={"status": "importRequested"},
        headers=headers,
        timeout=30
    )
    
    if response.status_code in [200, 202, 204]:
        log_success("Import requested")
    else:
        log_error(f"Failed to request import (status {response.status_code})")
        try:
            error_data = response.json()
            log_error(f"Error details: {json.dumps(error_data, indent=2)}")
        except:
            log_error(f"Response: {response.text}")
        sys.exit(EXIT_SYSTEM_ERROR)

# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    """Main function to orchestrate the upload process."""
    
    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Upload CSV files to harpin AI (Requires Python 3.7+)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Requirements:
  Python 3.7 or higher

Environment Variables Required:
  HARPIN_CLIENT_ID      Client ID for authentication
  HARPIN_REFRESH_TOKEN  Refresh token for authentication

Exit Codes:
  0 - Success (import completed)
  1 - User error (bad arguments, file not found, invalid sourceId, auth failure)
  2 - System error (API errors, network failures after retries, upload failed)

Example:
  python3 upload_to_harpin.py vMiY4q data_2026_01_06.csv
        """
    )
    parser.add_argument('sourceId', help='Source ID for the upload')
    parser.add_argument('fileName', help='Path to the CSV file to upload')
    
    args = parser.parse_args()
    
    source_id = args.sourceId
    file_path_str = args.fileName
    
    # Track overall start time
    overall_start_time = time.time()
    
    print("=" * 60)
    print("harpin AI CSV Upload Script")
    print("=" * 60)
    
    try:
        # Step 1: Authenticate
        access_token = get_access_token()
        
        # Step 2: Validate file
        file_path, file_size = validate_file(file_path_str)
        
        # Step 3: Validate source
        validate_source(source_id, access_token)
        
        # Step 4: Check concurrent uploads
        check_concurrent_uploads(source_id, access_token)
        
        # Step 5: Create upload
        upload_id, presigned_url = create_upload(source_id, file_path.name, access_token)
        
        # Step 6: Upload to S3
        upload_to_s3(presigned_url, file_path, file_size)
        
        # Step 7: Poll for analysis completion
        analysis_data = poll_status(source_id, upload_id, access_token, 'analysisCompleted', 'analysis')
        
        # Step 8: Request import
        request_import(source_id, upload_id, access_token)
        
        # Step 9: Poll for import completion
        final_data = poll_status(source_id, upload_id, access_token, 'importCompleted', 'import')
        
        # Display final summary
        total_records = final_data.get('totalRecords', 0)
        imported_records = final_data.get('importedRecords', 0)
        duration = time.time() - overall_start_time
        
        print("\n" + "=" * 60)
        print("UPLOAD SUMMARY")
        print("=" * 60)
        log_success(f"File: {file_path.name}")
        log_success(f"Total Records: {total_records}")
        log_success(f"Imported Records: {imported_records}")
        log_success(f"Duration: {duration:.1f} seconds")
        print("=" * 60)
        
        sys.exit(EXIT_SUCCESS)
        
    except KeyboardInterrupt:
        log_error("\nUpload interrupted by user")
        sys.exit(EXIT_SYSTEM_ERROR)
    except Exception as e:
        log_error(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(EXIT_SYSTEM_ERROR)

# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == '__main__':
    main()
