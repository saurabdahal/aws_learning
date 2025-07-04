import boto3


class Bucket:
    def __init__(self):
        self.bucket_name = ""
        self.s3 = boto3.client('s3')

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




