"""Loopback-only operations dashboard using the Python standard HTTP server."""
from __future__ import annotations

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import threading
from urllib.parse import parse_qs, urlsplit

from app.operations_status import OperationsStatus, creation_ticks


BASE = Path(__file__).resolve().parent


class LocalServer(ThreadingHTTPServer):
    allow_reuse_address = False

    def server_bind(self):
        if os.name == 'nt':
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        super().server_bind()


def make_handler(status, assets):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'SmartAPI-Operations'

        def respond(self, code, content, content_type):
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(content)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Referrer-Policy', 'no-referrer')
            self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'")
            try:
                self.end_headers()
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionAbortedError, ConnectionResetError):
                pass  # A browser navigation can cancel a pending refresh.

        def do_GET(self):
            host = self.headers.get('Host', '').split(':')[0].lower()
            if host not in ('127.0.0.1', 'localhost'):
                return self.respond(403, b'Local access only', 'text/plain; charset=utf-8')
            path = urlsplit(self.path)
            if path.path == '/api/health':
                return self.respond(200, b'{"app":"smartapi-operations","ok":true}', 'application/json')
            if path.path == '/favicon.ico':
                return self.respond(204, b'', 'image/x-icon')
            if path.path == '/api/status':
                try:
                    selected = parse_qs(path.query).get('date', [None])[0]
                    result = status.snapshot(selected)
                    return self.respond(200, json.dumps(result).encode(), 'application/json; charset=utf-8')
                except ValueError:
                    return self.respond(400, b'{"error":"Choose a valid session date."}', 'application/json')
                except Exception as error:
                    print('Status snapshot failed:', type(error).__name__, flush=True)
                    return self.respond(503, b'{"error":"Status is temporarily unavailable. Retrying does not change collection."}', 'application/json')
            names = {'/': ('index.html', 'text/html'), '/app.js': ('app.js', 'text/javascript'),
                     '/styles.css': ('styles.css', 'text/css')}
            if path.path not in names:
                return self.respond(404, b'Not found', 'text/plain')
            name, content_type = names[path.path]
            return self.respond(200, (assets / name).read_bytes(), content_type + '; charset=utf-8')

        def do_POST(self):
            self.respond(405, b'Read-only dashboard', 'text/plain')

        def log_message(self, *args):
            pass
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8502)
    args = parser.parse_args()
    status = OperationsStatus(BASE)
    server = LocalServer(('127.0.0.1', args.port), make_handler(status, BASE / 'operations'))
    record = BASE / 'runtime' / 'operations.process.json'
    record.parent.mkdir(exist_ok=True)
    record.write_text(json.dumps({'Id': os.getpid(), 'Name': 'operations-dashboard',
                                  'StartTicks': str(creation_ticks(os.getpid()))}), encoding='utf-8')
    def remember_sessions():
        while True:
            try:
                status.snapshot()
            except Exception as error:
                print('Daily snapshot unavailable:', type(error).__name__, flush=True)
            threading.Event().wait(60)
    threading.Thread(target=remember_sessions, daemon=True).start()
    print(f'Operations dashboard: http://127.0.0.1:{args.port}', flush=True)
    server.serve_forever()


if __name__ == '__main__':
    main()
