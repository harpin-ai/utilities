#!/usr/bin/env python3
"""
Batch automation script for harpin AI S3 uploads.

Reads a YAML config file mapping filename patterns to harpin AI source IDs,
surveys the configured S3 bucket, and uploads qualifying files in chronological
order by calling upload_to_harpin_s3.py for each one.

Processing rules:
  - Files are matched against patterns defined in the YAML config
  - Files whose date window exceeds max_window_days are skipped (catchup files)
  - Files are processed oldest-first (by start date) within each source
  - Sources are processed serially — one file at a time, one source at a time
  - Exit 1 from core script (bad file): log and continue to next file
  - Exit 2 from core script (infrastructure error): stop all processing immediately

REQUIRES: Python 3.7 or higher

Usage:
    python3 upload_to_harpin_s3_batch.py <config.yaml> [options]

Environment Variables Required:
    HARPIN_CLIENT_ID      - Client ID for authentication
    HARPIN_REFRESH_TOKEN  - Refresh token for authentication

AWS credentials follow the standard boto3 credential chain:
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
    → ~/.aws/credentials  → IAM instance/task role
"""

import sys
if sys.version_info < (3, 7):
    print("Error: This script requires Python 3.7 or higher.", file=sys.stderr)
    sys.exit(1)

import os
import re
import logging
import argparse
import subprocess
from datetime import datetime, date
from typing import List, Dict, Any, Tuple, Optional

try:
    import boto3
    from botocore.exceptions import ClientError as BotoClientError, NoCredentialsError
except ImportError:
    print("Error: 'boto3' is required. Install with: pip3 install boto3", file=sys.stderr)
    sys.exit(1)

try:
    import yaml
except ImportError:
    print("Error: 'pyyaml' is required. Install with: pip3 install pyyaml", file=sys.stderr)
    sys.exit(1)

# ============================================================================
# CONSTANTS
# ============================================================================

EXIT_SUCCESS    = 0
EXIT_USER_ERROR = 1
EXIT_SYS_ERROR  = 2

UPLOADER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "upload_to_harpin_s3.py")

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
# CONFIG
# ============================================================================

def load_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error("Config file not found: %s", path)
        sys.exit(EXIT_USER_ERROR)
    except yaml.YAMLError as e:
        logger.error("Failed to parse config file: %s", e)
        sys.exit(EXIT_USER_ERROR)

    # Validate required settings
    settings = config.get('settings', {})
    required = ['bucket', 'prefix', 'processed_prefix', 'failed_prefix', 'max_window_days']
    missing = [k for k in required if k not in settings]
    if missing:
        logger.error("Config missing required settings: %s", ", ".join(missing))
        sys.exit(EXIT_USER_ERROR)

    sources = config.get('sources', [])
    if not sources:
        logger.error("Config contains no sources")
        sys.exit(EXIT_USER_ERROR)

    return config

# ============================================================================
# SOURCE VALIDATION
# ============================================================================

def validate_sources_against_harpin(sources: List[Dict], access_token: str):
    """Confirm every source_id in the config exists and is flatFile type."""
    import requests as req

    API_BASE_URL = "https://api.harpin.ai"
    headers = {"Authorization": f"Bearer {access_token}",
               "Content-Type": "application/json"}

    logger.info("Validating %d source ID(s) against harpin AI...", len(sources))
    all_valid = True

    for source in sources:
        source_id   = source['source_id']
        source_name = source['name']
        try:
            r = req.get(f"{API_BASE_URL}/sources/{source_id}",
                        headers=headers, timeout=30)
            if r.status_code == 200:
                system = r.json().get('sourceSystem', 'unknown')
                if system != 'flatFile':
                    logger.error("Source '%s' (%s) has type '%s', must be flatFile",
                                 source_name, source_id, system)
                    all_valid = False
                else:
                    logger.debug("Source '%s' (%s) validated", source_name, source_id)
            elif r.status_code == 404:
                logger.error("Source '%s' (%s) not found in harpin", source_name, source_id)
                all_valid = False
            else:
                logger.error("Could not validate source '%s' (%s): HTTP %d",
                             source_name, source_id, r.status_code)
                all_valid = False
        except Exception as e:
            logger.error("Error validating source '%s' (%s): %s", source_name, source_id, e)
            all_valid = False

    if not all_valid:
        logger.error("One or more sources failed validation — aborting")
        sys.exit(EXIT_USER_ERROR)

    logger.info("All sources validated")


def get_access_token() -> str:
    import requests as req

    API_BASE_URL = "https://api.harpin.ai"
    client_id     = os.environ.get('HARPIN_CLIENT_ID', '').strip()
    refresh_token = os.environ.get('HARPIN_REFRESH_TOKEN', '').strip()

    missing = [v for v, val in [('HARPIN_CLIENT_ID', client_id),
                                 ('HARPIN_REFRESH_TOKEN', refresh_token)] if not val]
    if missing:
        logger.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(EXIT_USER_ERROR)

    try:
        r = req.post(f"{API_BASE_URL}/token",
                     json={"clientId": client_id, "refreshToken": refresh_token},
                     headers={"Content-Type": "application/json"},
                     timeout=30)
        if r.status_code == 200:
            token = r.json().get('accessToken')
            if not token:
                logger.error("Authentication response missing accessToken")
                sys.exit(EXIT_USER_ERROR)
            return token
        else:
            logger.error("Authentication failed (HTTP %d): %s", r.status_code, r.text)
            sys.exit(EXIT_USER_ERROR)
    except Exception as e:
        logger.error("Authentication request failed: %s", e)
        sys.exit(EXIT_USER_ERROR)

# ============================================================================
# BUCKET SURVEY
# ============================================================================

def parse_dates(start_str: str, end_str: str) -> Tuple[Optional[date], Optional[date]]:
    """Parse YYYYMMDD strings into date objects. Returns (None, None) on failure."""
    try:
        start = datetime.strptime(start_str, "%Y%m%d").date()
        end   = datetime.strptime(end_str,   "%Y%m%d").date()
        return start, end
    except ValueError:
        return None, None


def survey_bucket(s3: Any, bucket: str, prefix: str,
                  sources: List[Dict], max_window_days: int,
                  processed_prefix: str) -> Dict[str, List[Dict]]:
    """
    List objects in bucket/prefix and match against source patterns.
    Returns a dict of source_name → sorted list of file dicts.
    """
    logger.info("Surveying s3://%s/%s...", bucket, prefix)

    # Collect all object keys at the root prefix (exclude subprefixes like processed/, staging/)
    try:
        paginator = s3.get_paginator("list_objects_v2")
        all_objects = {}
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Only consider keys directly under the prefix (no subfolders)
                relative = key[len(prefix):]
                if "/" in relative:
                    continue
                if relative:
                    all_objects[relative] = obj
    except NoCredentialsError:
        logger.error("No AWS credentials found")
        sys.exit(EXIT_SYS_ERROR)
    except BotoClientError as e:
        logger.error("Failed to list bucket: %s", e)
        sys.exit(EXIT_SYS_ERROR)

    logger.info("Found %d object(s) at root prefix", len(all_objects))

    # Match each object against source patterns
    work_queues: Dict[str, List[Dict]] = {}

    for filename, obj in all_objects.items():
        matched = False
        for source in sources:
            m = re.match(source['pattern'], filename)
            if not m:
                continue

            matched = True
            start_date, end_date = parse_dates(m.group(1), m.group(2))
            if start_date is None:
                logger.warning("Could not parse dates from filename: %s — skipping", filename)
                break

            effective_max = source.get('max_window_days', max_window_days)
            window_days = (end_date - start_date).days
            if window_days > effective_max:
                logger.info("Skipping %s — window %d days exceeds max %d (catchup file)",
                            filename, window_days, effective_max)
                break

            entry = {
                "filename":   filename,
                "key":        obj["Key"],
                "s3_uri":     f"s3://{bucket}/{obj['Key']}",
                "source_id":  source['source_id'],
                "source_name": source['name'],
                "start_date": start_date,
                "end_date":   end_date,
                "window_days": window_days,
                "size":       obj["Size"],
            }

            work_queues.setdefault(source['name'], []).append(entry)
            break

        if not matched:
            logger.debug("No pattern match for: %s — ignoring", filename)

    # Sort each source queue oldest-first by start date, tiebreak by end date
    for name in work_queues:
        work_queues[name].sort(key=lambda f: (f['start_date'], f['end_date']))

    total = sum(len(q) for q in work_queues.values())
    logger.info("Matched %d file(s) across %d source(s)", total, len(work_queues))

    return work_queues


def format_size(size_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"

# ============================================================================
# PRINT PLAN
# ============================================================================

def print_plan(work_queues: Dict[str, List[Dict]]):
    """Print a human-readable processing plan."""
    total = sum(len(q) for q in work_queues.values())
    print(f"\nProcessing plan — {total} file(s) across {len(work_queues)} source(s)\n")

    for source_name, files in work_queues.items():
        print(f"  {source_name} ({len(files)} file(s)):")
        for i, f in enumerate(files, 1):
            print(f"    {i:>3}. {f['filename']}  "
                  f"({f['window_days']}d window, {format_size(f['size'])})")
        print()

# ============================================================================
# PROCESSING
# ============================================================================

def call_uploader(file: Dict, settings: Dict, extra_args: List[str],
                  dry_run: bool) -> int:
    """
    Call upload_to_harpin_s3.py for a single file.
    Returns the exit code.
    """
    cmd = [
        sys.executable, UPLOADER,
        file['source_id'],
        file['s3_uri'],
        "--processed-prefix", settings['processed_prefix'],
        "--failed-prefix",    settings['failed_prefix'],
    ]

    if 'analysis_timeout' in settings:
        cmd += ["--analysis-timeout", str(settings['analysis_timeout'])]
    if 'import_timeout' in settings:
        cmd += ["--import-timeout", str(settings['import_timeout'])]
    if 'aws_region' in settings:
        cmd += ["--aws-region", settings['aws_region']]

    cmd += extra_args

    if dry_run:
        cmd.append("--dry-run")

    logger.debug("Running: %s", " ".join(cmd))
    result = subprocess.run(cmd)
    return result.returncode


def process_queues(work_queues: Dict[str, List[Dict]], settings: Dict,
                   extra_args: List[str], dry_run: bool) -> int:
    """
    Process all queues serially. Returns an overall exit code.

    Exit 1 from uploader (bad file): log and continue to next file.
    Exit 2 from uploader (infrastructure error): stop everything immediately.
    """
    total       = sum(len(q) for q in work_queues.values())
    processed   = 0
    failed      = 0
    skipped     = 0

    for source_name, files in work_queues.items():
        logger.info("--- Source: %s (%d file(s)) ---", source_name, len(files))
        source_aborted = False

        for f in files:
            logger.info("Processing %s  (%s)", f['filename'], format_size(f['size']))

            exit_code = call_uploader(f, settings, extra_args, dry_run)

            if exit_code == EXIT_SUCCESS:
                processed += 1
                logger.info("OK: %s", f['filename'])

            elif exit_code == EXIT_USER_ERROR:
                failed += 1
                logger.warning("File error (exit 1): %s — continuing to next file",
                               f['filename'])

            elif exit_code == EXIT_SYS_ERROR:
                skipped += total - processed - failed - 1
                logger.error("Infrastructure error (exit 2): %s", f['filename'])
                logger.error("Stopping all processing — %d file(s) remaining unprocessed",
                             total - processed - failed)
                logger.error("Files remain at root and will be retried on next run")
                _print_summary(total, processed, failed, skipped)
                return EXIT_SYS_ERROR

            else:
                failed += 1
                logger.warning("Unexpected exit code %d from uploader for %s — continuing",
                               exit_code, f['filename'])

    _print_summary(total, processed, failed, skipped)
    return EXIT_SUCCESS if failed == 0 else EXIT_USER_ERROR


def _print_summary(total: int, processed: int, failed: int, skipped: int):
    logger.info("─" * 60)
    logger.info("Run summary: %d total — %d processed, %d failed, %d skipped",
                total, processed, failed, skipped)
    logger.info("─" * 60)

# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Batch S3 uploader for harpin AI (requires Python 3.7+)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables Required:
  HARPIN_CLIENT_ID      Client ID for authentication
  HARPIN_REFRESH_TOKEN  Refresh token for authentication

AWS credentials: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
  (or ~/.aws/credentials, or IAM role)

Exit Codes:
  0 — All files processed successfully
  1 — One or more files failed (bad file/config error)
  2 — Infrastructure error — processing stopped, retry on next run

Example:
  python3 upload_to_harpin_s3_batch.py sycuan_sources.yaml
  python3 upload_to_harpin_s3_batch.py sycuan_sources.yaml --dry-run
        """
    )

    parser.add_argument('config',   help='Path to YAML config file')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print processing plan and run pre-flight checks only — no uploads, no file moves')
    parser.add_argument('--aws-region',            default=None)
    parser.add_argument('--aws-access-key-id',     dest='aws_access_key_id',     default=None)
    parser.add_argument('--aws-secret-access-key', dest='aws_secret_access_key', default=None)
    parser.add_argument('--aws-session-token',     dest='aws_session_token',     default=None)
    parser.add_argument('--log-level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'])

    args = parser.parse_args()
    setup_logging(args.log_level)

    if args.dry_run:
        logger.info("DRY RUN — no uploads or file moves will be performed")

    # ── Load config ──────────────────────────────────────────────────────────
    config   = load_config(args.config)
    settings = config['settings']
    sources  = config['sources']

    logger.info("Loaded %d source(s) from %s", len(sources), args.config)
    logger.info("Max window: %d days", settings['max_window_days'])

    # ── Authenticate and validate sources ────────────────────────────────────
    logger.info("Authenticating with harpin AI...")
    token = get_access_token()
    logger.info("Authentication succeeded")
    validate_sources_against_harpin(sources, token)

    # ── Build boto3 client ───────────────────────────────────────────────────
    boto_kwargs = {"region_name": settings.get('aws_region', 'us-east-1')}
    if args.aws_access_key_id:
        boto_kwargs["aws_access_key_id"]     = args.aws_access_key_id
        boto_kwargs["aws_secret_access_key"] = args.aws_secret_access_key
        if args.aws_session_token:
            boto_kwargs["aws_session_token"] = args.aws_session_token
    s3 = boto3.client("s3", **boto_kwargs)

    # ── Survey bucket ────────────────────────────────────────────────────────
    work_queues = survey_bucket(
        s3,
        settings['bucket'],
        settings['prefix'],
        sources,
        settings['max_window_days'],
        settings['processed_prefix']
    )

    if not work_queues:
        logger.info("No files to process — nothing to do")
        sys.exit(EXIT_SUCCESS)

    # ── Print plan ───────────────────────────────────────────────────────────
    print_plan(work_queues)

    if args.dry_run:
        logger.info("Dry run complete — %d file(s) would be processed",
                    sum(len(q) for q in work_queues.values()))
        sys.exit(EXIT_SUCCESS)

    # ── Build extra args to pass through to uploader ─────────────────────────
    extra_args = []
    if args.aws_access_key_id:
        extra_args += ["--aws-access-key-id",     args.aws_access_key_id,
                       "--aws-secret-access-key", args.aws_secret_access_key]
        if args.aws_session_token:
            extra_args += ["--aws-session-token", args.aws_session_token]
    if args.aws_region:
        extra_args += ["--aws-region", args.aws_region]
    extra_args += ["--log-level", args.log_level]

    # ── Process ──────────────────────────────────────────────────────────────
    exit_code = process_queues(work_queues, settings, extra_args, dry_run=False)
    sys.exit(exit_code)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.error("Interrupted — files at root will be retried on next run")
        sys.exit(EXIT_SYS_ERROR)
