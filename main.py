from s3.BucketManager import BucketManager
from s3.FileManager import FileManager

if __name__ == '__main__':
    fm = FileManager()
    # FileManager().upload_single_file('annual-enterprise.csv')
    fm.apply_s3_lifecycle_configuration()
    # fm.delete_rule_by_id("DeleteUploadsAfter3Days")

