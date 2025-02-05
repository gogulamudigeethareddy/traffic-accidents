import os
import sys
import boto3
import unzip
from kaggle.api.kaggle_api_extended import KaggleApi

from configs.data_config import DATASET_IDENTIFIER, bucket_name, s3_directory

# Initialize Kaggle API
api = KaggleApi()
api.authenticate()

# Construct the path dynamically
base_path = os.path.dirname(os.path.abspath(__file__))
new_path = os.path.join(base_path, "data")

# Download dataset to the specified path
for dataset in DATASET_IDENTIFIER:
    api.dataset_download_files(dataset, path=new_path, unzip=True)

print(f"Dataset downloaded to: {new_path}")


def upload_to_s3(new_path, bucket_name, s3_directory):
    # Initialize S3 client
    s3 = boto3.client("s3")

    # Check if the bucket exists, and create it if it doesn't
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except boto3.exceptions.botocore.client.ClientError:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} created.")

    # Upload the dataset to S3
    for root, dirs, files in os.walk(new_path):
        for file in files:
            s3_path = os.path.join(s3_directory, file)
            s3.upload_file(os.path.join(root, file), bucket_name, s3_path)
            print(f"Uploaded {file} to S3")


# Upload the dataset to S3
upload_to_s3(new_path, bucket_name, s3_directory)
