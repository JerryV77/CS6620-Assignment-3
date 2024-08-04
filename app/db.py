import boto3
import os
import time
from botocore.exceptions import ClientError

class Database:
    def __init__(self, table_name='ItemsTable'):
        endpoint_url = os.getenv('DYNAMODB_ENDPOINT')
        region_name = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name, endpoint_url=endpoint_url)
        self.table = self.dynamodb.Table(table_name)
        self._create_table()

    def _create_table(self):
        retry_attempts = 5
        while retry_attempts > 0:
            try:
                self.table.load()
                break
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
                    break
                else:
                    print(e.response['Error']['Message'])
                    retry_attempts -= 1
                    time.sleep(5)
                    if retry_attempts == 0:
                        raise

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
        except ClientError as e:
            print(e.response['Error']['Message'])

    def delete_item(self, item_id):
        try:
            self.table.delete_item(Key={'id': item_id})
        except ClientError as e:
            print(e.response['Error']['Message'])