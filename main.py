from s3.BucketManager import BucketManager

if __name__ == '__main__':
    BucketManager().create_bucket(region="us-east-2")

