from s3.BucketManager import BucketManager
from s3.FileManager import FileManager

if __name__ == '__main__':
    # FileManager().upload_single_file('annual-enterprise.csv')
    FileManager().upload_directory()

