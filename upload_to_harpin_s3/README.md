# upload_to_harpin_s3

A Python utility for uploading CSV files from an S3 bucket to harpin AI using the [harpin AI public APIs](https://harpin.ai/docs/api.html). The file is streamed directly from S3 into the harpin upload API — no temporary files, flat memory usage regardless of file size.

## Requirements
- **Python 3.7 or higher**
  - `boto3>=1.26.0`
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

## Usage
```
python3 upload_to_harpin_s3.py <sourceId> <s3Uri>
```

### Arguments
- **sourceId** — the ID of the harpin AI source to upload to (must be a `flatFile` source type)
- **s3Uri** — fully-qualified S3 URI of the CSV file (`s3://bucket/key.csv`)

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
python3 upload_to_harpin_s3.py srcId123 s3://my-bucket/my_file.csv
```

## Features
- **S3 streaming** — streams directly from S3 into the presigned PUT with no intermediate storage
- **File validation** — confirms file exists, is readable, is a `.csv`, and is within the 5 GB size limit
- **Source validation** — confirms the source ID exists and is of type `flatFile`
- **Concurrent upload checking** — prevents exceeding the maximum concurrent upload limit
- **Progress tracking** — progress bar during upload (suppressed when not running in a TTY)
- **Automatic retry** — retries network operations up to 3 times with exponential backoff
- **Status monitoring** — polls upload status through analysis and import phases with configurable timeouts
- **File lifecycle management** — moves processed files to `processed/` or `failed/` with a JSON sidecar log
- **Dry run mode** — validates all pre-flight checks without making API calls or moving files

## File Lifecycle
After a completed run the file is moved from its original location to either `processed/` or `failed/`, alongside a JSON sidecar `.log` file. Transient failures (network errors, timeouts) leave the file in place so the next run can retry.

```
s3://bucket/filename.csv          (original location)
         │
         │ import completed successfully
         ▼
processed/filename.csv
processed/filename.csv.log

         │ unrecoverable error (bad CSV, invalid source)
         ▼
failed/filename.csv
failed/filename.csv.log

         │ transient failure (network error, timeout)
         ▼
s3://bucket/filename.csv          (file stays in place — no sidecar)
```

## Exit Codes
| Code | Meaning |
|---|---|
| `0` | Success — import completed |
| `1` | User/configuration error — bad arguments, invalid source, authentication failure, bad file |
| `2` | Infrastructure/system error — network failures after retries, timeouts, S3 permission errors |

## Limitations
- **Maximum file size:** 5 GB
- **Maximum concurrent uploads:** 3 uploads per source
- **File type:** CSV files only (for `flatFile` sources)

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
- Ensure your AWS credentials have `s3:GetObject`, `s3:PutObject`, `s3:CopyObject`, and `s3:DeleteObject` on the relevant prefixes
