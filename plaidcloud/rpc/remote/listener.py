#!/usr/bin/env python
# coding=utf-8

"""Sets up a listening server to wait for incoming auth code redirects"""
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler
from socketserver import TCPServer


class AuthCodeListener(BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server, listen_path, state=None):
        """Overriding default init to add listen_path and state"""
        self.listen_path = listen_path
        self.state = state
        self.keep_listening = True    # This will be set to false once we have a code
        BaseHTTPRequestHandler.__init__(self, request, client_address, server)

    def do_GET(self):
        if not self.path.startswith(self.listen_path):
            # Not a valid path. Don't really need to do anything with this.
            self.send_response(404)

        parsed_path = urlparse(self.path)
        query_args = parse_qs(parsed_path.query)

        state = query_args.get('state')
        if state != self.state:
            # State mixup. Not valid.
            self.send_response(401)

        self.code = query_args['code']
        self.keep_listening = False

        self.send_response(200)


def get_auth_code(hostname='localhost', port=8080, listen_path='/'):
    """Listens for an incoming auth code redirect

    This will probably need to be called in a thread so that an initial request
    can be sent while the redirect listener is listening.

    Args:
        hostname (str, optional): The hostname to listen on.
        port (int, optional): What port to listen on
        listen_path (str, optional): What path to listen on"""

    httpd = TCPServer((hostname, port), AuthCodeListener)
    while httpd.RequestHandlerClass.keep_listening:
        httpd.handle_request()

    return httpd.RequestHandlerClass.code