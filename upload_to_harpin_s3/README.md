# upload_to_harpin_s3

Python utilities for uploading CSV files from an S3 bucket to harpin AI using the [harpin AI public APIs](https://harpin.ai/docs/api.html).

Two scripts are provided:

- **`upload_to_harpin_s3.py`** — uploads a single file, given a source ID and S3 URI
- **`upload_to_harpin_s3_batch.py`** — surveys a bucket, matches files against a YAML config, and calls the single-file script in chronological order

Both scripts stream files directly from S3 into the harpin upload API — no temporary files, flat memory usage regardless of file size.

## Requirements
- **Python 3.7 or higher**
  - `boto3>=1.26.0`
  - `pyyaml>=6.0`
  - `requests>=2.31.0`
  - `tqdm>=4.66.0`

## Installation
```
pip3 install -r requirements.txt
```

Set up harpin AI authentication credentials as described in the [parent README](../README.md#setup---authentication). You'll need:
- `HARPIN_CLIENT_ID` — your harpin AI client ID
- `HARPIN_REFRESH_TOKEN` — your harpin AI refresh token

AWS credentials follow the standard boto3 credential chain:
1. Explicit CLI flags (`--aws-access-key-id` / `--aws-secret-access-key` / `--aws-session-token`)
2. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
3. AWS config/credentials files (`~/.aws/`)
4. IAM instance/task role

---

## upload_to_harpin_s3.py — single file

Uploads one CSV or CSV.GZ file from S3 to harpin AI. Intended for manual uploads, targeted reruns, and as the execution unit called by the batch script.

### Usage
```
python3 upload_to_harpin_s3.py <sourceId> <s3Uri>
```

### Arguments
- **sourceId** — the ID of the harpin AI source to upload to (must be a `flatFile` source type)
- **s3Uri** — fully-qualified S3 URI of the file (`s3://bucket/key.csv` or `s3://bucket/key.csv.gz`)

### Options
| Flag | Default | Description |
|---|---|---|
| `--processed-prefix` | `processed/` | Destination prefix for successfully processed files |
| `--failed-prefix` | `failed/` | Destination prefix for files with unrecoverable errors |
| `--analysis-timeout` | `1800` | Seconds to wait for analysis completion (30 min) |
| `--import-timeout` | `7200` | Seconds to wait for import completion (2 hrs) |
| `--put-timeout` | `600` | Seconds for the presigned PUT before timing out (10 min) |
| `--dry-run` | `False` | Run all pre-flight checks only — no API calls, no file moves |
| `--aws-region` | `us-east-1` | AWS region for the S3 bucket |
| `--aws-access-key-id` | — | Explicit AWS access key |
| `--aws-secret-access-key` | — | Explicit AWS secret key |
| `--aws-session-token` | — | Explicit session token for temporary credentials |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### Example
```
python3 upload_to_harpin_s3.py srcId123 s3://my-bucket/my_file.csv.gz
```

### Features
- **S3 streaming** — streams directly from S3 into the presigned PUT with no intermediate storage
- **File validation** — confirms file exists, is readable, is a `.csv` or `.csv.gz`, and is within the 5 GB size limit
- **Source validation** — confirms the source ID exists and is of type `flatFile`
- **Concurrent upload checking** — prevents exceeding the maximum concurrent upload limit
- **Progress tracking** — progress bar during upload (suppressed when not running in a TTY)
- **Automatic retry** — retries network operations up to 3 times with exponential backoff
- **Status monitoring** — polls upload status through analysis and import phases with configurable timeouts
- **File lifecycle management** — moves processed files to `processed/` or `failed/` with a JSON sidecar log
- **Dry run mode** — validates all pre-flight checks without making API calls or moving files

### File Lifecycle
After a completed run the file is moved from its original location to either `processed/` or `failed/`, alongside a JSON sidecar `.log` file. Transient failures (network errors, timeouts) leave the file in place so the next run can retry.

```
s3://bucket/filename.csv.gz       (original location)
         │
         │ import completed successfully
         ▼
processed/filename.csv.gz
processed/filename.csv.gz.log

         │ unrecoverable error (bad file, invalid source)
         ▼
failed/filename.csv.gz
failed/filename.csv.gz.log

         │ transient failure (network error, timeout)
         ▼
s3://bucket/filename.csv.gz       (file stays in place — no sidecar)
```

### Exit Codes
| Code | Meaning |
|---|---|
| `0` | Success — import completed |
| `1` | User/configuration error — bad arguments, invalid source, authentication failure, bad file |
| `2` | Infrastructure/system error — network failures after retries, timeouts, S3 permission errors |

---

## upload_to_harpin_s3_batch.py — batch automation

Surveys an S3 bucket, matches files against patterns defined in a YAML config, and uploads qualifying files in chronological order. Designed to run as a cron job or scheduled task.

### Usage
```
python3 upload_to_harpin_s3_batch.py <config.yaml> [options]
```

### Arguments
- **config.yaml** — path to a YAML config file mapping filename patterns to harpin source IDs (see [Configuration](#configuration) below)

### Options
| Flag | Default | Description |
|---|---|---|
| `--dry-run` | `False` | Print the processing plan and validate all sources — no uploads or file moves |
| `--aws-region` | `us-east-1` | AWS region for the S3 bucket |
| `--aws-access-key-id` | — | Explicit AWS access key |
| `--aws-secret-access-key` | — | Explicit AWS secret key |
| `--aws-session-token` | — | Explicit session token for temporary credentials |
| `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |

### Example
```
# Preview what would be processed
python3 upload_to_harpin_s3_batch.py sample_sources.yaml --dry-run

# Run for real
python3 upload_to_harpin_s3_batch.py sample_sources.yaml
```

### Processing behaviour
- Files are matched against patterns defined in the YAML config — anything not matched is silently ignored
- Files whose date window exceeds `max_window_days` are skipped and logged (catchup files)
- Matched files are processed oldest-first within each source, sources are processed serially
- **Exit `1` from uploader** (bad file): logged, processing continues with the next file
- **Exit `2` from uploader** (infrastructure error): all processing stops immediately — files remain at root and will be retried on the next run

### Exit Codes
| Code | Meaning |
|---|---|
| `0` | All files processed successfully |
| `1` | One or more files failed (bad file or configuration error) |
| `2` | Infrastructure error — processing stopped, retry on next run |

---

## Configuration

The YAML config file defines the S3 bucket settings and the list of sources to process. A sample config is provided in `sample_sources.yaml`.

### Settings

| Key | Description |
|---|---|
| `bucket` | S3 bucket name |
| `prefix` | Prefix to survey for files (e.g. `tenant/`) |
| `processed_prefix` | Destination prefix for successfully processed files |
| `failed_prefix` | Destination prefix for files with unrecoverable errors |
| `max_window_days` | Maximum date window in days — files with a wider span are treated as catchups and skipped |

### Sources

Each source entry requires:
- **`name`** — human-readable label used in logs
- **`pattern`** — Python regex with exactly two capture groups: start date and end date (`YYYYMMDD`)
- **`source_id`** — harpin AI `flatFile` source ID
- **`max_window_days`** *(optional)* — overrides the global `max_window_days` for this source only. Useful for sources with a longer cadence (weekly, monthly) that would otherwise be skipped by the global threshold.

### Example config

```yaml
settings:
  bucket: my-bucket
  prefix: my-prefix/
  processed_prefix: my-prefix/processed/
  failed_prefix: my-prefix/failed/
  max_window_days: 7

sources:

  - name: File Pattern 1
    pattern: ^FILE_PATTERN_1_(\d{8})_(\d{8})\.csv\.gz$
    source_id: sourceId1

  - name: File Pattern 2
    pattern: ^FILE_PATTERN_2_(\d{8})_(\d{8})\.csv\.gz$
    source_id: sourceId2
```

---

## IAM Permissions

The AWS credentials used must have the following permissions on the bucket:

| Operation | Required for |
|---|---|
| `s3:ListBucket` | Surveying the bucket for files to process |
| `s3:GetObject` | Streaming file content to the harpin upload API |
| `s3:PutObject` | Writing sidecar `.log` files to `processed/` and `failed/` |
| `s3:CopyObject` | Moving files to `processed/` or `failed/` |
| `s3:DeleteObject` | Removing the original file after a successful copy |

---

## Troubleshooting

### Authentication Errors
- Verify `HARPIN_CLIENT_ID` and `HARPIN_REFRESH_TOKEN` are set correctly
- Confirm you have network connectivity to `https://api.harpin.ai`

### Invalid Source ID
- The script will display available `flatFile` sources when an invalid ID is provided
- Ensure the source is of type `flatFile` — other source types are not supported

### Concurrent Upload Limit
- Wait for existing uploads to complete before retrying
- The limit is 3 concurrent uploads per source

### S3 Permission Errors
- The script emits an actionable error message identifying the exact operation and bucket
- Check that your AWS credentials have all five permissions listed in the IAM section above

### Files Not Being Picked Up
- Confirm the filename matches the pattern in your YAML config exactly (patterns are anchored with `^` and `$`)
- Check that the date window does not exceed `max_window_days` — run with `--log-level DEBUG` to see skipped files
- Confirm the file is at the root of the configured `prefix`, not inside a subfolder
