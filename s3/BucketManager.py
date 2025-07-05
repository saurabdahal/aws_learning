import boto3
from botocore.exceptions import ClientError
import os


class BucketManager:
    def __init__(self):
        # please keep in mind following things while choosing a bucket name
        # 1. Bucket names must be 3 to 63 characters and unique within the global namespace
        # 2. Bucket names must also begin and end with a letter or number and must be lowercase
        # 3. Valid characters are a-z, 0-9, periods (.), and hyphens (-)
        self.bucket_name = "aws-demo-fruit"  # name must be unique globally
        self.s3 = boto3.client('s3')
        self.data_dir = "data"

    def create_bucket(self, region, bucket_name=None):
        try:
            if not bucket_name:
                bucket_name = self.bucket_name
            self.s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
            # you can create bucket in any region you like
        except ClientError as e:
            print(f"Error: {e.response['Error']['Message']}")

    def list_all_buckets(self):
        try:
            resp = self.s3.list_buckets()
            buckets = resp.get('Buckets', [])
            if buckets:
                for b in buckets:
                    print(b)
            else:
                print("no buckets found")
        except ClientError as e:
            print(f"Error: {e.response['Error']['Message']}")






