import os,sys
import threading
from boto3.s3.transfer import TransferConfig, MB, KB

import boto3


class FileManager:
    def __init__(self):
        self.bucket_name = "aws-demo-fruit"
        self.s3 = boto3.client('s3')
        self.data_dir = "/home/saurab/my_projects/aws_projects/data"
        self.filesize = 0
        self.uploaded = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount, filename):
        with self._lock:
            self.uploaded += bytes_amount
            upload_percentage = (self.uploaded / self.filesize) * 100
            print(f"file : {filename} : {self.uploaded} / {self.filesize} ----- {upload_percentage}% ")

    def upload_single_file(self, filename=None):
        if filename is None:
            filename = self.filename
        self.filesize = float(os.path.getsize(os.path.join(self.data_dir, filename)))

        # TransferConfig allows us to define the parameters for multipart.
        # Since default threshold is larger than our filesize we will use smaller threshold to force
        # boto3 to use multipart. We will use 100KB chunk size
        transfer_config = TransferConfig(
            multipart_threshold=100 * MB,
            multipart_chunksize=100 * KB
        )
        try:
            self.s3.upload_file(
                Filename=os.path.join(self.data_dir, filename),
                Bucket=self.bucket_name,
                Key=filename,
                Callback=lambda bytes_transferred: self.__call__(bytes_transferred, filename),
                Config=transfer_config
            )

            # We can verify the upload completion by checking ETag
            # Note: For multipart uploads, response ETag will contain a dash and part count
            response = self.s3.head_object(Bucket=self.bucket_name, Key=filename)
            etag = response['ETag']
            print(etag)
        except Exception as e:
            print(f"Error: {e.args}")
