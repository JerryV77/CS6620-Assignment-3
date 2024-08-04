import unittest
import json
from http.server import HTTPServer
from threading import Thread
import requests
from app.server import RequestHandler
import boto3
import os

class TestServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        server = HTTPServer(('localhost', 8000), RequestHandler)
        cls.server_thread = Thread(target=server.serve_forever)
        cls.server_thread.daemon = True
        cls.server_thread.start()

    def setUp(self):
        self.clear_database_and_s3()

    def clear_database_and_s3(self):
        # Clear DynamoDB Table
        dynamodb = boto3.resource('dynamodb', endpoint_url=os.getenv('DYNAMODB_ENDPOINT'))
        table = dynamodb.Table('ItemsTable')
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(Key={'id': each['id']})
        
        # Clear S3 Bucket
        s3 = boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT'))
        bucket_name = 'my-bucket'
        try:
            objects = s3.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
        except s3.exceptions.NoSuchBucket:
            pass

    def get_s3_object(self, key):
        s3 = boto3.client('s3', endpoint_url=os.getenv('S3_ENDPOINT'))
        try:
            obj = s3.get_object(Bucket='my-bucket', Key=key)
            return json.loads(obj['Body'].read().decode('utf-8'))
        except s3.exceptions.NoSuchKey:
            return None

    def get_dynamodb_item(self, item_id):
        dynamodb = boto3.resource('dynamodb', endpoint_url=os.getenv('DYNAMODB_ENDPOINT'))
        table = dynamodb.Table('ItemsTable')
        response = table.get_item(Key={'id': item_id})
        return response.get('Item')

    def test_get_item(self):
        response = requests.get('http://localhost:8000/item/1')
        self.assertEqual(response.status_code, 404)

    def test_get_item_not_found(self):
        response = requests.get('http://localhost:8000/item/nonexistent')
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()['error'], 'Item not found')

    def test_get_item_no_parameters(self):
        response = requests.get('http://localhost:8000/item/')
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['error'], 'Item ID not provided')

    def test_get_item_invalid_parameters(self):
        response = requests.get('http://localhost:8000/item/invalid')
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()['error'], 'Item not found')

    def test_post_item(self):
        data = {'id': '1', 'name': 'Item 1'}
        response = requests.post('http://localhost:8000/item', json=data)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()['message'], 'Item created')

        # Verify DynamoDB Item
        dynamodb_item = self.get_dynamodb_item('1')
        self.assertIsNotNone(dynamodb_item)
        self.assertEqual(dynamodb_item['id'], '1')
        self.assertEqual(dynamodb_item['name'], 'Item 1')

        # Verify S3 Object
        s3_item = self.get_s3_object('1')
        self.assertIsNotNone(s3_item)
        self.assertEqual(s3_item, data)

    def test_post_duplicate_item(self):
        data = {'id': '1', 'name': 'Item 1'}
        response = requests.post('http://localhost:8000/item', json=data)
        self.assertEqual(response.status_code, 201)
        
        response = requests.post('http://localhost:8000/item', json=data)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()['error'], 'Item already exists')

    def test_put_item(self):
        # Ensure the item is created before attempting to update it
        data = {'id': '1', 'name': 'Item 1'}
        response = requests.post('http://localhost:8000/item', json=data)
        self.assertEqual(response.status_code, 201)

        # Update the item
        updated_data = {'id': '1', 'name': 'Item 1 Updated'}
        response = requests.put('http://localhost:8000/item', json=updated_data)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['message'], 'Item updated')

        # Verify DynamoDB Item
        dynamodb_item = self.get_dynamodb_item('1')
        self.assertIsNotNone(dynamodb_item)
        self.assertEqual(dynamodb_item['id'], '1')
        self.assertEqual(dynamodb_item['name'], 'Item 1 Updated')

        # Verify S3 Object
        s3_item = self.get_s3_object('1')
        self.assertIsNotNone(s3_item)
        self.assertEqual(s3_item, updated_data)

    def test_put_item_no_target(self):
        data = {'id': 'nonexistent', 'name': 'Nonexistent Item'}
        response = requests.put('http://localhost:8000/item', json=data)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()['error'], 'Item not found')

    def test_delete_item(self):
        # Ensure the item is created before attempting to delete it
        data = {'id': '1', 'name': 'Item to Delete'}
        response = requests.post('http://localhost:8000/item', json=data)
        self.assertEqual(response.status_code, 201)
        
        response = requests.delete('http://localhost:8000/item/1')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['message'], 'Item deleted')

        # Verify DynamoDB Item
        dynamodb_item = self.get_dynamodb_item('1')
        self.assertIsNone(dynamodb_item)

        # Verify S3 Object
        s3_item = self.get_s3_object('1')
        self.assertIsNone(s3_item)

    def test_delete_item_no_target(self):
        response = requests.delete('http://localhost:8000/item/nonexistent')
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()['error'], 'Item not found')

    @classmethod
    def tearDownClass(cls):
        try:
            requests.get('http://localhost:8000/shutdown')
        except requests.exceptions.ConnectionError:
            pass

if __name__ == '__main__':
    unittest.main()
