#import airflow
from airflow.hooks.S3_hook import S3Hook


class Uploads3:
    def __init__(self,filename,key,bucket_name):
        self.filename = filename
        self.key = key
        self.bucket_name = bucket_name


    def upload_to_s3(self):


        hook = S3Hook('s3_conn')
        hook.load_file(filename=self.filename, key=self.key, bucket_name=self.bucket_name,acl_policy='public-read') 

