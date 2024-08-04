import unittest
import json
from http.server import HTTPServer
from threading import Thread
import requests
from app.server import RequestHandler
from app.s3 import S3Bucket  # Assuming this is the correct import path
from app.db import Database  # Assuming this is the correct import path
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
        s3 = boto3.client('s3', endpoint_url=os.getenv('DYNAMODB_ENDPOINT'))
        bucket_name = 'my-bucket'
        try:
            objects = s3.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
        except s3.exceptions.NoSuchBucket:
            pass

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

        # Validate with S3 and DynamoDB
        s3_item = json.loads(S3Bucket().get_object('1').decode('utf-8'))
        db_item = Database().get_item('1')

        self.assertEqual(db_item['id'], '1')
        self.assertEqual(db_item['name'], 'Item 1')
        self.assertEqual(s3_item['id'], '1')
        self.assertEqual(s3_item['name'], 'Item 1')

        response = requests.get('http://localhost:8000/item/1')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['id'], '1')
        self.assertEqual(response.json()['name'], 'Item 1')

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

        # Validate with S3 and DynamoDB
        s3_item = json.loads(S3Bucket().get_object('1').decode('utf-8'))
        db_item = Database().get_item('1')

        self.assertEqual(db_item['id'], '1')
        self.assertEqual(db_item['name'], 'Item 1 Updated')
        self.assertEqual(s3_item['id'], '1')
        self.assertEqual(s3_item['name'], 'Item 1 Updated')

        response = requests.get('http://localhost:8000/item/1')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['name'], 'Item 1 Updated')

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

        # Validate with S3 and DynamoDB
        s3_item = S3Bucket().get_object('1')
        db_item = Database().get_item('1')

        self.assertIsNone(db_item)
        self.assertIsNone(s3_item)

        response = requests.get('http://localhost:8000/item/1')
        self.assertEqual(response.status_code, 404)

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
