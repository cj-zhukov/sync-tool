# sync-tool
sync-tool is a Rust library for uploading files from local storage (Windows/Linux/Mac) to cloud storage (like AWS S3, etc).

## Description
sync-tool calculates differents between file names and sizes of source and target folder and upload to the cloud.

## Installation
Use the package manager cargo or docker to install sync-tool.

## Config file sync-tool.json
- `bucket`: str, target bucket for upload, example: "bucket_name"
- `region`: str, region name, example: "region_name"
- `source`: str, data source example: "path/to/data/"
- `target`: str, data target in AWS S3, example: "path/to/data/"
- `workers`: int, count files for uploading in parallel, example: 10
- `chunk_size`: int, size in MiB, if less then whole file will be uploaded, else by chunk, example: 10
- `max_chunks`: int, count of chunks for file, describes max file to be uploaded, example: 10000
- `files_to_ignore`: arr of str, list of files to be ignored, example: [".DS_Store"]
- `retries`: int, how many times try to reupload file, example: 5
- `chunk_retries`: int, how many times try to reupload chunk for the file, example: 5
- `chunk_workers`: int, count chunks for the file for uploading in parallel, example: 10
- `check_size`: bool, check final size of uploded file (can be expensive), example: true

## Max file size
chunk_size (MiB) * max_chunks (int) = max file size

## Modes
- dif - (dry-run), calculate and show dif
- upload - upload files without checking target file names and sizes (files in target if exist will be overwritten)
- sync - smart check file name and size before uploading

## Usage
```bash
# sync-tool mode
# mode: app mode dif | upload | sync
# config-name: sync-tool.json is default name
./sync-tool sync
```