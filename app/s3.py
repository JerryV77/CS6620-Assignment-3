import boto3
import os
import time
from botocore.exceptions import ClientError

class S3Bucket:
    def __init__(self, bucket_name='my-bucket'):
        endpoint_url = os.getenv('S3_ENDPOINT', 'http://localstack:4566')
        region_name = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.s3 = boto3.client('s3', region_name=region_name, endpoint_url=endpoint_url)
        self.bucket_name = bucket_name
        self._create_bucket()

    def _create_bucket(self):
        retry_attempts = 5
        while retry_attempts > 0:
            try:
                self.s3.head_bucket(Bucket=self.bucket_name)
                break
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    self.s3.create_bucket(Bucket=self.bucket_name)
                    break
                else:
                    print(e.response['Error']['Message'])
                    retry_attempts -= 1
                    time.sleep(5)
                    if retry_attempts == 0:
                        raise

    def put_object(self, key, data):
        try:
            self.s3.put_object(Bucket=self.bucket_name, Key=key, Body=data)
        except ClientError as e:
            print(e.response['Error']['Message'])
    
    def get_object(self, key):
        try:
            response = self.s3.get_object(Bucket=self.bucket_name, Key=key)
            return response['Body'].read()
        except ClientError as e:
            print(e.response['Error']['Message'])
            return None

    def delete_object(self, key):
        try:
            self.s3.delete_object(Bucket=self.bucket_name, Key=key)
        except ClientError as e:
            print(e.response['Error']['Message'])
