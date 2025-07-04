import boto3


class BucketManager:
    def __init__(self):
        self.bucket_name = "aws-demo-fruit"  # name must be unique globally
        self.s3 = boto3.client('s3')

    def create_bucket(self, bucket_name=None):
        try:
            if not bucket_name:
                bucket_name = self.bucket_name
            print(bucket_name)
            self.s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'us-east-2'})
            # you can create bucket in any region you like
        except Exception as e:
            print(f"Error: {e.args}")

    def list_buckets(self):
        try:
            resp = self.s3.list_buckets()
            buckets = resp.get('Buckets', [])
            if buckets:
                for b in buckets:
                    print(b)
            else:
                print("no buckets found")
        except Exception as e:
            print(f"Error: {e.args}")




