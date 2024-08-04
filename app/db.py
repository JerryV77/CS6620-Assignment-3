import boto3
import os
from botocore.exceptions import ClientError
from app.s3 import S3Bucket

class Database:
    def __init__(self, table_name='ItemsTable'):
        endpoint_url = os.getenv('DYNAMODB_ENDPOINT')
        region_name = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url)
        self.table = self.dynamodb.Table(table_name)
        self.s3 = S3Bucket()
        self._create_table()

    def _create_table(self):
        try:
            self.table.load()
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                self.table = self.dynamodb.create_table(
                    TableName=self.table.name,
                    KeySchema=[
                        {'AttributeName': 'id', 'KeyType': 'HASH'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'id', 'AttributeType': 'S'}
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 10,
                        'WriteCapacityUnits': 10
                    }
                )
                self.table.meta.client.get_waiter('table_exists').wait(TableName=self.table.name)

    def get_item(self, item_id):
        try:
            response = self.table.get_item(Key={'id': item_id})
            return response.get('Item')
        except ClientError as e:
            print(e.response['Error']['Message'])
            return None

    def put_item(self, item):
        try:
            self.table.put_item(Item=item)
            self.s3.put_object(item['id'], json.dumps(item))
        except ClientError as e:
            print(e.response['Error']['Message'])

    def delete_item(self, item_id):
        try:
            self.table.delete_item(Key={'id': item_id})
            self.s3.delete_object(item_id)
        except ClientError as e:
            print(e.response['Error']['Message'])
