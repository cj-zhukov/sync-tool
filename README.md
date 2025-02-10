# sync-tool
sync-tool is a Rust library for uploading files from local storage (Windows/Linux/Mac) into AWS S3.

## Description
sync-tool calculates differents between file names and sizes of source and target folder and upload to the cloud.

## Installation
Use the package manager cargo or docker to install sync-tool.

## Config file sync-tool.json
```json
    "bucket": str target bucket for upload example: "bucket"
    "region": str region name example: "region"
    "source": str data source example: "path/to/data/"
    "target": str data target in AWS S3 example: "path/to/data/"
    "workers": int count files for uploading in parallel example: 10
    "chunk_size": int size in MiB, if less then whole file will be uploaded, else by chunk example: 10
    "max_chunks": int count of chunks for file, describes max file to be uploaded example: 10000
```

## Max file size
chunk_size (MiB) * max_chunks = max file size

## Usage
```bash
# sync-tool mode
# mode: app mode dif | show | upload | sync
# config-name: sync-tool.json must exist with the binary
./sync-tool sync
```