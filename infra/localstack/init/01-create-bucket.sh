#!/bin/bash
set -e

echo "Creating S3 bucket: event-viewer-local"
awslocal s3 mb s3://event-viewer-local
echo "S3 bucket created successfully"
