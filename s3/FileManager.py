import os,sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

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
            multipart_threshold=100 * KB,
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

    def upload_directory(self, max_workers: int = 4):

        files_list = []
        for root, _, files in os.walk(self.data_dir):
            for f in files:
                files_list.append(os.path.join(root, f))

        if not files_list:
            print(f" No files in {self.data_dir} ?")
            return

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(self.upload_single_file, os.path.relpath(path, self.data_dir)): path
                for path in files_list
            }

            """
                We use as_completed() here for futures. It is an iterator over the given futures that yields each as it
                completes. [Doc]. as_completed() takes a list (or any iterable) of Future objects and returns an 
                iterator that yields each future as soon as it finishes, in no particular order. 
            """
            for future in as_completed(futures):
                src_path = futures[future]
                try:
                    future.result()
                    print(f"Uploaded: {src_path}")
                except Exception as e:
                    print(f"Failed to upload {src_path}: {e}")
