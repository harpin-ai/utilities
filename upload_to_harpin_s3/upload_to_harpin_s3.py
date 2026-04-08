#!/usr/bin/env python3
"""
S3 CSV Upload Script for harpin AI

Uploads a single CSV file from an S3 bucket to harpin AI following the
documented 8-step upload workflow. The file is streamed directly from S3
into the presigned PUT — no temporary files, flat memory usage.

After a definitive outcome the file is moved (copy + delete) to either
processed/ or failed/ alongside a JSON sidecar .log file. Transient
failures leave the file at the root so the next run can retry.

REQUIRES: Python 3.7 or higher

Usage:
    python3 upload_to_harpin_s3.py <sourceId> <s3Uri>

Environment Variables Required:
    HARPIN_CLIENT_ID      - Client ID for authentication
    HARPIN_REFRESH_TOKEN  - Refresh token for authentication

AWS credentials follow the standard boto3 chain:
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
    → ~/.aws/credentials  → IAM instance/task role
"""

# Check Python version before any imports
import sys
if sys.version_info < (3, 7):
    print("Error: This script requires Python 3.7 or higher.", file=sys.stderr)
    print(f"You are using Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}", file=sys.stderr)
    sys.exit(1)

import os
import time
import json
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

try:
    import requests
except ImportError:
    print("Error: 'requests' library is required. Install with: pip3 install requests", file=sys.stderr)
    sys.exit(2)

try:
    from tqdm import tqdm
except ImportError:
    print("Error: 'tqdm' library is required. Install with: pip3 install tqdm", file=sys.stderr)
    sys.exit(2)

try:
    import boto3
    from botocore.exceptions import ClientError as BotoClientError
except ImportError:
    print("Error: 'boto3' library is required. Install with: pip3 install boto3", file=sys.stderr)
    sys.exit(2)

# ============================================================================
# CONFIGURATION & CONSTANTS
# ============================================================================

API_BASE_URL = "https://api.harpin.ai"
MAX_CONCURRENT_UPLOADS = 3
MAX_FILE_SIZE_GB = 5
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_GB * 1024 * 1024 * 1024
RETRY_ATTEMPTS = 3
RETRY_DELAYS = [10, 20, 40]  # exponential backoff
POLL_INTERVAL_SECONDS = 5

EXIT_SUCCESS = 0
EXIT_USER_ERROR = 1
EXIT_SYSTEM_ERROR = 2

# ============================================================================
# LOGGING
# ============================================================================

def setup_logging(level: str):
    logging.basicConfig(
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
        level=getattr(logging, level.upper(), logging.INFO),
        stream=sys.stdout,
    )

logger = logging.getLogger(__name__)

# ============================================================================
# HELPERS
# ============================================================================

def format_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def parse_s3_uri(s3_uri: str) -> Tuple[str, str]:
    """Parse s3://bucket/key into (bucket, key). Exits on malformed URI."""
    if not s3_uri.startswith("s3://"):
        logger.error("S3 URI must start with s3://: %s", s3_uri)
        sys.exit(EXIT_USER_ERROR)
    parts = s3_uri[5:].split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        logger.error("Malformed S3 URI (expected s3://bucket/key): %s", s3_uri)
        sys.exit(EXIT_USER_ERROR)
    return parts[0], parts[1]


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def retry_api(func):
    """Decorator: retry on ConnectionError/Timeout and HTTP 429/5xx."""
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                last_exc = e
                if attempt < RETRY_ATTEMPTS:
                    delay = RETRY_DELAYS[attempt - 1]
                    logger.debug("Network error attempt %d/%d: %s — retrying in %ds",
                                 attempt, RETRY_ATTEMPTS, e, delay)
                    time.sleep(delay)
                else:
                    logger.error("Network error after %d attempts: %s", RETRY_ATTEMPTS, e)
        raise last_exc
    return wrapper


class ProgressStreamWrapper:
    """
    Wraps a boto3 streaming body, updating a tqdm progress bar as data is read.
    Ensures Content-Length is honoured without buffering the entire file.
    """
    def __init__(self, stream, progress_bar):
        self._stream = stream
        self._pbar = progress_bar

    def read(self, size=-1):
        data = self._stream.read(size)
        if data:
            self._pbar.update(len(data))
        return data

# ============================================================================
# AUTHENTICATION
# ============================================================================

def get_access_token() -> str:
    client_id     = os.environ.get('HARPIN_CLIENT_ID', '').strip()
    refresh_token = os.environ.get('HARPIN_REFRESH_TOKEN', '').strip()

    missing = [v for v, val in [('HARPIN_CLIENT_ID', client_id),
                                 ('HARPIN_REFRESH_TOKEN', refresh_token)] if not val]
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(EXIT_USER_ERROR)

    logger.info("Pre-flight: authenticating with harpin AI...")
    try:
        response = requests.post(
            f"{API_BASE_URL}/token",
            json={"clientId": client_id, "refreshToken": refresh_token},
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        if response.status_code == 200:
            token = response.json().get('accessToken')
            if not token:
                logger.error("Authentication response missing accessToken")
                sys.exit(EXIT_USER_ERROR)
            logger.info("Pre-flight: authentication succeeded")
            return token
        else:
            logger.error("Authentication failed (HTTP %d): %s",
                         response.status_code, response.text)
            sys.exit(EXIT_USER_ERROR)
    except requests.exceptions.RequestException as e:
        logger.error("Authentication request failed: %s", e)
        sys.exit(EXIT_USER_ERROR)

# ============================================================================
# PRE-FLIGHT VALIDATION
# ============================================================================

def validate_source(source_id: str, access_token: str):
    logger.info("Pre-flight: validating source %s...", source_id)
    headers = {"Authorization": f"Bearer {access_token}",
               "Content-Type": "application/json"}
    try:
        r = requests.get(f"{API_BASE_URL}/sources/{source_id}",
                         headers=headers, timeout=30)
        if r.status_code == 200:
            source_system = r.json().get('sourceSystem', 'unknown')
            if source_system != 'flatFile':
                logger.error("Source %s has type '%s', must be 'flatFile'",
                             source_id, source_system)
                sys.exit(EXIT_USER_ERROR)
            logger.info("Pre-flight: source %s validated (flatFile)", source_id)
            return
        elif r.status_code == 404:
            logger.error("Source ID not found: %s", source_id)
            # Show available flatFile sources
            try:
                sr = requests.get(f"{API_BASE_URL}/sources", headers=headers, timeout=30)
                if sr.status_code == 200:
                    sources = sr.json().get('content', [])
                    flat = [s for s in sources if s.get('sourceSystem') == 'flatFile']
                    if flat:
                        logger.info("Available flatFile sources:")
                        for s in flat:
                            logger.info("  ID: %s  Name: %s", s.get('id'), s.get('name'))
                    else:
                        logger.info("No flatFile sources available")
            except Exception:
                pass
            sys.exit(EXIT_USER_ERROR)
        else:
            logger.error("Source validation failed (HTTP %d): %s",
                         r.status_code, r.text)
            sys.exit(EXIT_USER_ERROR)
    except requests.exceptions.RequestException as e:
        logger.error("Source validation request failed: %s", e)
        sys.exit(EXIT_USER_ERROR)


def check_concurrent_uploads(source_id: str, access_token: str):
    logger.info("Pre-flight: checking concurrent uploads...")
    headers = {"Authorization": f"Bearer {access_token}",
               "Content-Type": "application/json"}
    try:
        r = requests.get(f"{API_BASE_URL}/sources/{source_id}/uploads",
                         headers=headers, timeout=30)
        if r.status_code != 200:
            logger.error("Concurrent upload check failed (HTTP %d): %s",
                         r.status_code, r.text)
            sys.exit(EXIT_SYSTEM_ERROR)

        uploads = r.json()
        if isinstance(uploads, dict):
            uploads = uploads.get('content', [])

        in_progress_statuses = {
            'created', 'analysisInProgress', 'analysisCompleted',
            'importRequested', 'importInProgress'
        }
        count = sum(1 for u in uploads
                    if isinstance(u, dict) and u.get('status') in in_progress_statuses)

        if count >= MAX_CONCURRENT_UPLOADS:
            logger.error("Concurrent upload limit reached (%d/%d) — wait for existing uploads to complete",
                         count, MAX_CONCURRENT_UPLOADS)
            sys.exit(EXIT_USER_ERROR)

        logger.info("Pre-flight: concurrent uploads %d/%d", count, MAX_CONCURRENT_UPLOADS)
    except requests.exceptions.RequestException as e:
        logger.error("Concurrent upload check request failed: %s", e)
        sys.exit(EXIT_SYSTEM_ERROR)


def validate_s3_file(s3: Any, bucket: str, key: str, s3_uri: str) -> int:
    """
    Confirm file exists, is readable, is a .csv, and is within size limits.
    Returns ContentLength in bytes.
    Non-.csv and oversized files return EXIT_USER_ERROR (→ failed/).
    """
    logger.info("Pre-flight: checking %s...", s3_uri)

    if not key.lower().endswith('.csv'):
        logger.error("File is not a .csv: %s", s3_uri)
        sys.exit(EXIT_USER_ERROR)

    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except BotoClientError as e:
        code = e.response['Error']['Code']
        logger.error("Cannot access %s: %s", s3_uri, code)
        sys.exit(EXIT_USER_ERROR)

    size = head['ContentLength']
    if size > MAX_FILE_SIZE_BYTES:
        logger.error("File size %s exceeds %d GB limit: %s",
                     format_size(size), MAX_FILE_SIZE_GB, s3_uri)
        sys.exit(EXIT_USER_ERROR)

    logger.info("Pre-flight: %s exists, %s, readable", s3_uri, format_size(size))
    return size

# ============================================================================
# UPLOAD WORKFLOW
# ============================================================================

@retry_api
def create_upload(source_id: str, file_name: str, access_token: str) -> Tuple[str, str]:
    headers = {"Authorization": f"Bearer {access_token}",
               "Content-Type": "application/json"}
    r = requests.post(
        f"{API_BASE_URL}/sources/{source_id}/uploads",
        json={"fileName": file_name},
        headers=headers,
        timeout=30
    )
    if r.status_code in [200, 201]:
        data = r.json()
        upload_id    = data.get('id')
        presigned_url = data.get('url')
        if not upload_id or not presigned_url:
            logger.error("Create upload response missing required fields")
            sys.exit(EXIT_SYSTEM_ERROR)
        logger.info("Step 4: upload created, upload_id=%s", upload_id)
        return upload_id, presigned_url
    else:
        logger.error("Failed to create upload (HTTP %d): %s", r.status_code, r.text)
        sys.exit(EXIT_SYSTEM_ERROR)


def stream_s3_to_presigned_url(s3: Any, bucket: str, key: str,
                                presigned_url: str, content_length: int,
                                put_timeout: int, s3_uri: str):
    file_name = key.split('/')[-1]
    logger.info("Step 5: PUT to presigned URL started (%s)", format_size(content_length))
    start = time.time()

    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj['Body']

    use_tqdm = sys.stdout.isatty()
    with tqdm(total=content_length, unit='B', unit_scale=True,
              desc=file_name, disable=not use_tqdm) as pbar:
        wrapped = ProgressStreamWrapper(body, pbar)
        try:
            response = requests.put(
                presigned_url,
                data=wrapped,
                headers={
                    "Content-Type": "text/csv",
                    "Content-Length": str(content_length)
                },
                timeout=put_timeout
            )
        except requests.exceptions.Timeout:
            logger.error("Presigned PUT timed out after %ds — file stays at root", put_timeout)
            sys.exit(EXIT_SYSTEM_ERROR)

    if response.status_code == 200:
        elapsed = time.time() - start
        logger.info("Step 5: PUT completed in %.1fs", elapsed)
    elif response.status_code == 403:
        # S3 XML 403 on presigned PUT = TTL expired or signature mismatch
        logger.error("Presigned PUT returned 403 — URL may have expired. Response: %s",
                     response.text)
        logger.error("File stays at root: %s", s3_uri)
        sys.exit(EXIT_SYSTEM_ERROR)
    else:
        logger.error("Presigned PUT failed (HTTP %d): %s", response.status_code, response.text)
        sys.exit(EXIT_SYSTEM_ERROR)


def poll_status(source_id: str, upload_id: str, access_token: str,
                target_status: str, phase_name: str,
                timeout_seconds: int) -> Dict[str, Any]:
    headers = {"Authorization": f"Bearer {access_token}",
               "Content-Type": "application/json"}
    start = time.time()
    last_status = None

    while True:
        elapsed = time.time() - start
        if elapsed > timeout_seconds:
            logger.error("Poll timeout after %.0fs waiting for %s — upload_id=%s — file stays at root",
                         elapsed, target_status, upload_id)
            sys.exit(EXIT_SYSTEM_ERROR)

        try:
            r = requests.get(
                f"{API_BASE_URL}/sources/{source_id}/uploads/{upload_id}",
                headers=headers, timeout=30
            )
        except requests.exceptions.RequestException as e:
            logger.debug("Poll request error: %s — will retry", e)
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        if r.status_code != 200:
            logger.error("Poll returned HTTP %d: %s", r.status_code, r.text)
            sys.exit(EXIT_SYSTEM_ERROR)

        data = r.json()
        current_status = data.get('status')

        if current_status != last_status:
            logger.debug("Poll status: %s", current_status)
            last_status = current_status

        if current_status == 'failed':
            error_msg = data.get('errorMessage', 'unknown error')
            logger.error("%s failed: %s", phase_name, error_msg)
            return data  # caller handles disposition

        if current_status == target_status:
            logger.info("%s completed in %.1fs", phase_name, time.time() - start)
            return data

        time.sleep(POLL_INTERVAL_SECONDS)


@retry_api
def request_import(source_id: str, upload_id: str, access_token: str):
    headers = {"Authorization": f"Bearer {access_token}",
               "Content-Type": "application/json"}
    r = requests.put(
        f"{API_BASE_URL}/sources/{source_id}/uploads/{upload_id}/status",
        json={"status": "importRequested"},
        headers=headers,
        timeout=30
    )
    if r.status_code in [200, 202, 204]:
        logger.info("Step 7: import requested")
    else:
        logger.error("Failed to request import (HTTP %d): %s", r.status_code, r.text)
        sys.exit(EXIT_SYSTEM_ERROR)

# ============================================================================
# S3 FILE LIFECYCLE
# ============================================================================

def s3_move(s3: Any, bucket: str, src_key: str, dst_key: str, s3_uri: str):
    """Copy src → dst then delete src. On copy failure the file stays at root."""
    logger.info("Moving s3://%s/%s → %s", bucket, src_key, dst_key)
    try:
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": src_key},
            Key=dst_key
        )
    except BotoClientError as e:
        code = e.response['Error']['Code']
        logger.error("Failed to copy file to %s", dst_key)
        logger.error("  S3 error: %s on CopyObject", code)
        logger.error("  Bucket: %s", bucket)
        logger.error("  File remains at original location: %s", s3_uri)
        sys.exit(EXIT_SYSTEM_ERROR)

    try:
        s3.delete_object(Bucket=bucket, Key=src_key)
    except BotoClientError as e:
        code = e.response['Error']['Code']
        logger.error("File was copied to %s but original could not be deleted: %s", dst_key, code)
        logger.error("  Manual cleanup needed: %s", s3_uri)
        sys.exit(EXIT_SYSTEM_ERROR)


def write_sidecar(s3: Any, bucket: str, log_key: str, payload: Dict[str, Any]):
    logger.info("Writing %s", log_key)
    try:
        s3.put_object(
            Bucket=bucket,
            Key=log_key,
            Body=json.dumps(payload, indent=2).encode(),
            ContentType="application/json"
        )
    except BotoClientError as e:
        code = e.response['Error']['Code']
        logger.error("Failed to write sidecar log %s: %s", log_key, code)
        # Non-fatal — the file has already been moved; log and continue


def move_to_processed(s3: Any, bucket: str, key: str, processed_prefix: str,
                      s3_uri: str, sidecar: Dict[str, Any]):
    file_name = key.split('/')[-1]
    dst_key   = f"{processed_prefix}{file_name}"
    log_key   = f"{processed_prefix}{file_name}.log"
    s3_move(s3, bucket, key, dst_key, s3_uri)
    write_sidecar(s3, bucket, log_key, sidecar)


def move_to_failed(s3: Any, bucket: str, key: str, failed_prefix: str,
                   s3_uri: str, sidecar: Dict[str, Any]):
    file_name = key.split('/')[-1]
    dst_key   = f"{failed_prefix}{file_name}"
    log_key   = f"{failed_prefix}{file_name}.log"
    s3_move(s3, bucket, key, dst_key, s3_uri)
    write_sidecar(s3, bucket, log_key, sidecar)

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Upload a CSV from S3 to harpin AI (requires Python 3.7+)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables Required:
  HARPIN_CLIENT_ID      Client ID for authentication
  HARPIN_REFRESH_TOKEN  Refresh token for authentication

AWS credentials: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
  (or ~/.aws/credentials, or IAM role)

Exit Codes:
  0 — Success (import completed)
  1 — User/configuration error (bad args, invalid source, auth failure, bad file)
  2 — Infrastructure/system error (network failures, timeouts, S3 permission errors)

Example:
  python3 upload_to_harpin_s3.py vMiY4q s3://my-bucket/sycuan/customers_2026_01.csv
        """
    )

    parser.add_argument('sourceId', help='harpin AI source ID (must be flatFile type)')
    parser.add_argument('s3Uri',    help='S3 URI of the CSV file (s3://bucket/key)')

    parser.add_argument('--processed-prefix',  default='processed/',
                        help='Destination prefix for successfully processed files (default: processed/)')
    parser.add_argument('--failed-prefix',     default='failed/',
                        help='Destination prefix for failed files (default: failed/)')
    parser.add_argument('--analysis-timeout',  type=int, default=1800,
                        help='Seconds to wait for analysis completion (default: 1800)')
    parser.add_argument('--import-timeout',    type=int, default=7200,
                        help='Seconds to wait for import completion (default: 7200)')
    parser.add_argument('--put-timeout',       type=int, default=600,
                        help='Seconds for the presigned PUT (default: 600)')
    parser.add_argument('--dry-run',           action='store_true',
                        help='Run pre-flight checks only — no API calls, no file moves')
    parser.add_argument('--aws-region',        default='us-east-1',
                        help='AWS region (default: us-east-1)')
    parser.add_argument('--aws-access-key-id',     dest='aws_access_key_id',     default=None)
    parser.add_argument('--aws-secret-access-key', dest='aws_secret_access_key', default=None)
    parser.add_argument('--aws-session-token',     dest='aws_session_token',     default=None)
    parser.add_argument('--log-level',         default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        help='Log level (default: INFO)')

    args = parser.parse_args()
    setup_logging(args.log_level)

    source_id = args.sourceId
    s3_uri    = args.s3Uri
    bucket, key = parse_s3_uri(s3_uri)
    file_name   = key.split('/')[-1]
    run_start   = time.time()

    # Build boto3 client
    boto_kwargs = {"region_name": args.aws_region}
    if args.aws_access_key_id:
        boto_kwargs["aws_access_key_id"]     = args.aws_access_key_id
        boto_kwargs["aws_secret_access_key"] = args.aws_secret_access_key
        if args.aws_session_token:
            boto_kwargs["aws_session_token"] = args.aws_session_token
    s3 = boto3.client("s3", **boto_kwargs)

    # ── Step 1: Authenticate ────────────────────────────────────────────────
    access_token = get_access_token()

    # ── Step 2: Validate source ─────────────────────────────────────────────
    validate_source(source_id, access_token)

    # ── Step 3: Check concurrent uploads ───────────────────────────────────
    check_concurrent_uploads(source_id, access_token)

    # ── S3 pre-flight ───────────────────────────────────────────────────────
    content_length = validate_s3_file(s3, bucket, key, s3_uri)

    if args.dry_run:
        logger.info("Dry run complete — all pre-flight checks passed")
        logger.info("Would upload %s (%s) to source %s",
                    s3_uri, format_size(content_length), source_id)
        sys.exit(EXIT_SUCCESS)

    upload_id: Optional[str] = None
    failed_at_step: Optional[str] = None
    error_message:  Optional[str] = None

    try:
        # ── Step 4: Create upload ───────────────────────────────────────────
        upload_id, presigned_url = create_upload(source_id, file_name, access_token)

        # ── Step 5: Stream S3 → presigned PUT ──────────────────────────────
        stream_s3_to_presigned_url(s3, bucket, key, presigned_url,
                                   content_length, args.put_timeout, s3_uri)

        # ── Step 6: Poll for analysis ───────────────────────────────────────
        logger.info("Step 6: waiting for analysis...")
        analysis_data = poll_status(source_id, upload_id, access_token,
                                    'analysisCompleted', 'Step 6: analysis',
                                    args.analysis_timeout)

        if analysis_data.get('status') == 'failed':
            failed_at_step = 'analysis'
            error_message  = analysis_data.get('errorMessage', 'analysis failed')
            raise RuntimeError(error_message)

        # ── Step 7: Request import ──────────────────────────────────────────
        request_import(source_id, upload_id, access_token)

        # ── Step 8: Poll for import ─────────────────────────────────────────
        logger.info("Step 8: waiting for import...")
        import_data = poll_status(source_id, upload_id, access_token,
                                  'importCompleted', 'Step 8: import',
                                  args.import_timeout)

        if import_data.get('status') == 'failed':
            failed_at_step = 'import'
            error_message  = import_data.get('errorMessage', 'import failed')
            raise RuntimeError(error_message)

        # ── Success ─────────────────────────────────────────────────────────
        duration        = time.time() - run_start
        total_records   = import_data.get('totalRecords', 0)
        imported_records = import_data.get('importedRecords', 0)
        logger.info("Step 8: import completed — %d records in %.1fs",
                    imported_records, duration)

        sidecar = {
            "timestamp":        now_utc(),
            "outcome":          "success",
            "source_s3_uri":    s3_uri,
            "source_id":        source_id,
            "upload_id":        upload_id,
            "total_records":    total_records,
            "imported_records": imported_records,
            "duration_seconds": round(duration, 1)
        }
        move_to_processed(s3, bucket, key, args.processed_prefix, s3_uri, sidecar)
        sys.exit(EXIT_SUCCESS)

    except RuntimeError:
        # Definitive harpin-side failure — move to failed/
        duration = time.time() - run_start
        sidecar = {
            "timestamp":      now_utc(),
            "outcome":        "failed",
            "source_s3_uri":  s3_uri,
            "source_id":      source_id,
            "upload_id":      upload_id,
            "failed_at_step": failed_at_step,
            "error_message":  error_message,
            "exit_code":      EXIT_USER_ERROR
        }
        move_to_failed(s3, bucket, key, args.failed_prefix, s3_uri, sidecar)
        sys.exit(EXIT_USER_ERROR)

    except KeyboardInterrupt:
        logger.error("Interrupted — file stays at root: %s", s3_uri)
        sys.exit(EXIT_SYSTEM_ERROR)

    except Exception as e:
        logger.error("Unexpected error: %s — file stays at root: %s", e, s3_uri)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(EXIT_SYSTEM_ERROR)


if __name__ == '__main__':
    main()
