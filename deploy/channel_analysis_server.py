#!/usr/bin/env python
# -*- coding: utf-8 -*-
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import json
import os
import ssl
import subprocess
import SocketServer
import threading
import urlparse

ROOT = os.environ.get('CHANNEL_ANALYSIS_ROOT', '/opt/channel-analysis')
HOST = os.environ.get('HOST', '0.0.0.0')
PORT = int(os.environ.get('PORT', '80'))
TLS_PORT = int(os.environ.get('TLS_PORT', '0'))
TLS_CERT = os.environ.get('TLS_CERT', '')
TLS_KEY = os.environ.get('TLS_KEY', '')
MAX_BODY_BYTES = 1024 * 1024
ALLOWED_ORIGIN = 'https://maolaoapi.com'
ALLOWED_PATHS = set(['/api/channel/', '/api/log/'])


class ThreadedHTTPServer(SocketServer.ThreadingMixIn, HTTPServer):
    daemon_threads = True


def response_headers(extra=None):
    headers = {
        'Cache-Control': 'no-store',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Accept',
    }
    if extra:
        headers.update(extra)
    return headers


def json_bytes(payload):
    return json.dumps(payload, ensure_ascii=False).encode('utf-8')


class ChannelAnalysisHandler(BaseHTTPRequestHandler):
    server_version = 'ChannelAnalysis/1.0'

    def log_message(self, fmt, *args):
        # Avoid logging request bodies or forwarded auth headers.
        return BaseHTTPRequestHandler.log_message(self, fmt, *args)

    def send_bytes(self, status, body, headers=None):
        self.send_response(status)
        for key, value in response_headers(headers).items():
            self.send_header(key, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, status, payload):
        self.send_bytes(status, json_bytes(payload), {
            'Content-Type': 'application/json; charset=utf-8',
        })

    def do_OPTIONS(self):
        self.send_bytes(204, b'')

    def do_GET(self):
        parsed = urlparse.urlparse(self.path)
        if parsed.path == '/healthz':
            return self.send_json(200, {'ok': True})
        if parsed.path not in ('/', '/index.html'):
            return self.send_json(404, {'success': False, 'message': 'Not found.'})

        index_path = os.path.join(ROOT, 'index.html')
        try:
            with open(index_path, 'rb') as handle:
                content = handle.read()
        except IOError:
            return self.send_json(500, {'success': False, 'message': 'index.html is missing.'})

        self.send_bytes(200, content, {
            'Content-Type': 'text/html; charset=utf-8',
            'Cache-Control': 'no-cache',
        })

    def do_POST(self):
        parsed = urlparse.urlparse(self.path)
        if parsed.path != '/proxy':
            return self.send_json(404, {'success': False, 'message': 'Not found.'})

        try:
            length = int(self.headers.get('Content-Length', '0'))
        except ValueError:
            length = 0
        if length <= 0 or length > MAX_BODY_BYTES:
            return self.send_json(400, {'success': False, 'message': 'Invalid request body size.'})

        try:
            payload = json.loads(self.rfile.read(length))
        except Exception:
            return self.send_json(400, {'success': False, 'message': 'Invalid JSON body.'})

        raw_url = payload.get('url') or ''
        headers = payload.get('headers') or {}
        cookie = (headers.get('Cookie') or headers.get('cookie') or '').strip()
        api_user = (headers.get('New-Api-User') or headers.get('new-api-user') or '').strip()

        parsed_url = urlparse.urlparse(raw_url)
        target_origin = '%s://%s' % (parsed_url.scheme, parsed_url.netloc)
        if target_origin != ALLOWED_ORIGIN or parsed_url.path not in ALLOWED_PATHS:
            return self.send_json(400, {'success': False, 'message': 'Only NewAPI channel and log endpoints are allowed.'})
        if not cookie or not api_user:
            return self.send_json(400, {'success': False, 'message': 'Cookie and New-Api-User are required.'})

        command = [
            '/usr/bin/curl',
            '-sS',
            '--max-time', '45',
            '-H', 'Accept: application/json',
            '-H', 'Cookie: %s' % cookie,
            '-H', 'New-Api-User: %s' % api_user,
            '-w', '\n__HTTP_STATUS__:%{http_code}',
            raw_url,
        ]
        try:
            output = subprocess.check_output(command, stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as error:
            output = error.output or b''
        marker = b'\n__HTTP_STATUS__:'
        body, sep, status = output.rpartition(marker)
        try:
            http_status = int(status.strip()) if sep else 502
        except ValueError:
            http_status = 502
            body = output

        self.send_bytes(http_status, body, {
            'Content-Type': 'application/json; charset=utf-8',
        })


if __name__ == '__main__':
    def make_server(port, certfile='', keyfile=''):
        server = ThreadedHTTPServer((HOST, port), ChannelAnalysisHandler)
        if certfile and keyfile:
            server.socket = ssl.wrap_socket(
                server.socket,
                certfile=certfile,
                keyfile=keyfile,
                server_side=True,
            )
        return server

    if TLS_PORT and TLS_CERT and TLS_KEY:
        http_server = make_server(PORT)
        http_thread = threading.Thread(target=http_server.serve_forever)
        http_thread.daemon = True
        http_thread.start()
        make_server(TLS_PORT, TLS_CERT, TLS_KEY).serve_forever()
    else:
        make_server(PORT).serve_forever()
