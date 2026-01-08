# upload_to_harpin

A Python utility for uploading CSV files to harpin AI using the [harpin AI public APIs](https://harpin.ai/docs/api.html). This script is designed for automated ETL pipelines, cron jobs, and manual uploads.

## Requirements
- **Python 3.7 or higher**
  - `requests>=2.31.0`
  - `tqdm>=4.66.0`

## Installation
- Set up authentication credentials as described in the [parent README](../README.md#setup---authentication). You'll need to set the following environment variables:
   - `HARPIN_CLIENT_ID` - Your harpin AI client ID
   - `HARPIN_REFRESH_TOKEN` - Your harpin AI refresh token

## Usage
`python3 upload_to_harpin.py <sourceId> <fileName>`

### Arguments
- **sourceId** - The ID of the harpin AI source to upload to (must be a `flatFile` source type)
- **fileName** - Path to the CSV file to upload

## Features
- **File Validation** - Verifies file exists, is readable, and within size limits before upload
- **Source Validation** - Confirms the source ID exists and is of type `flatFile`. If invalid, displays available flatFile sources
- **Concurrent Upload Checking** - Prevents exceeding the maximum concurrent upload limit
- **Progress Tracking** - Progress bar during file upload
- **Automatic Retry** - Retries network operations up to 3 times on failure
- **Status Monitoring** - Polls and displays upload status through analysis and import phases

## Exit Codes
The script uses the following exit codes:
- **0** - Success (import completed)
- **1** - User error (bad arguments, file not found, invalid sourceId, authentication failure)
- **2** - System error (API errors, network failures after retries, upload failed)

These exit codes make the script suitable for use in automated pipelines where you need to detect and handle different types of failures.

## Limitations
- **Maximum file size:** 5 GB
- **Maximum concurrent uploads:** 3 uploads per source
- **File type:** CSV files only (for flatFile sources)


## Troubleshooting
### Authentication Errors
If you receive authentication errors, verify that:
- Your `HARPIN_CLIENT_ID` and `HARPIN_REFRESH_TOKEN` environment variables are set correctly
- Your credentials haven't expired (regenerate refresh token if needed)
- You have network connectivity to `https://api.harpin.ai`

### Invalid Source ID
If you receive an "Invalid source ID" error:
- The script will automatically display available flatFile sources
- Verify you're using the correct source ID from the list
- Ensure the source is of type `flatFile` (other source types are not supported)

### Concurrent Upload Limit
If you receive a "Maximum concurrent uploads reached" error:
- Wait for existing uploads to complete
- Check the harpin AI web application to monitor upload progress
- The limit is 3 concurrent uploads per source

### File Size Errors
If your file exceeds the 5 GB limit:
- Split the file into smaller chunks
- Upload each chunk separately