# sync-tool
sync-tool is a Rust library for uploading files from local storage into AWS S3.

## Description
sync-tool calculates differents between file names and sizes of source and target folder and upload to the cloud.

## Installation
Use the package manager cargo or docker to install sync-tool.

## Usage
```bash
# sync-tool mode
# mode: app mode dif | show | upload | sync
# config-name: config json filepath sync-tool.json
./sync-tool sync
```
