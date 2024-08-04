import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from app.db import Database

class RequestHandler(BaseHTTPRequestHandler):
    db = Database()

    def _set_response(self, status=200):
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        try:
            if not self.path.startswith('/item'):
                self._set_response(400)
                self.wfile.write(json.dumps({'error': 'Invalid endpoint'}).encode())
                return

            item_id = self.path.split('/')[-1]
            if item_id:
                item = self.db.get_item(item_id)
                if item:
                    self._set_response()
                    self.wfile.write(json.dumps(item).encode())
                else:
                    self._set_response(404)
                    self.wfile.write(json.dumps({'error': 'Item not found'}).encode())
            else:
                self._set_response(400)
                self.wfile.write(json.dumps({'error': 'Item ID not provided'}).encode())
        except Exception as e:
            self._set_response(500)
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def do_POST(self):
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            item = json.loads(post_data)
            item_id = item.get('id')

            if not item_id:
                self._set_response(400)
                self.wfile.write(json.dumps({'error': 'Item ID not provided'}).encode())
                return

            if self.db.get_item(item_id):
                self._set_response(409)
                self.wfile.write(json.dumps({'error': 'Item already exists'}).encode())
                return

            self.db.put_item(item)
            self._set_response(201)
            self.wfile.write(json.dumps({'message': 'Item created'}).encode())
        except Exception as e:
            self._set_response(500)
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def do_PUT(self):
        try:
            content_length = int(self.headers['Content-Length'])
            put_data = self.rfile.read(content_length)
            item = json.loads(put_data)
            item_id = item.get('id')

            if not item_id:
                self._set_response(400)
                self.wfile.write(json.dumps({'error': 'Item ID not provided'}).encode())
                return

            if not self.db.get_item(item_id):
                self._set_response(404)
                self.wfile.write(json.dumps({'error': 'Item not found'}).encode())
                return

            self.db.put_item(item)
            self._set_response(200)
            self.wfile.write(json.dumps({'message': 'Item updated'}).encode())
        except Exception as e:
            self._set_response(500)
            self.wfile.write(json.dumps({'error': str(e)}).encode())

    def do_DELETE(self):
        try:
            item_id = self.path.split('/')[-1]
            if not item_id:
                self._set_response(400)
                self.wfile.write(json.dumps({'error': 'Item ID not provided'}).encode())
                return

            if not self.db.get_item(item_id):
                self._set_response(404)
                self.wfile.write(json.dumps({'error': 'Item not found'}).encode())
                return

            self.db.delete_item(item_id)
            self._set_response(200)
            self.wfile.write(json.dumps({'message': 'Item deleted'}).encode())
        except Exception as e:
            self._set_response(500)
            self.wfile.write(json.dumps({'error': str(e)}).encode())

def run(server_class=HTTPServer, handler_class=RequestHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting httpd on port {port}...')
    httpd.serve_forever()

if __name__ == '__main__':
    run()
