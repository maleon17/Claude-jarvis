#!/usr/bin/env python3
"""Tailscale Serve's --set-path mounts a path at the target's root -- it
strips the prefix before forwarding, so /ask, /upload and /reset would all
arrive at cmd_queue.py as a bare "/" and become indistinguishable if they
shared one port. This shim restores the distinction: one tiny listener per
logical endpoint, each on its own local port, blindly forwarding its root
path verbatim to the correct fixed path on cmd_queue.py (127.0.0.1:9092).
"""
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

CMDQ = "http://127.0.0.1:9092"

ROUTES = {
    9101: "/ask",
    9102: "/upload",
    9103: "/reset",
    9104: "/download",
    9105: "/classify",
    9106: "/tool_call_pending",
    9107: "/tool_call_result",
    9109: "/xask",
    9110: "/xreset",
}


def make_handler(target_path):
    class Handler(BaseHTTPRequestHandler):
        def _forward(self, method):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else None
            headers = {}
            if self.headers.get("Content-Type"):
                headers["Content-Type"] = self.headers.get("Content-Type")
            # Tailscale strips the mounted prefix from the path but the
            # query string (e.g. /download?path=...) survives on whatever
            # is left of self.path -- reattach it to the real fixed path.
            query = urlsplit(self.path).query
            full_path = target_path + (f"?{query}" if query else "")
            req = Request(CMDQ + full_path, data=body, method=method, headers=headers)
            try:
                with urlopen(req, timeout=55) as resp:
                    data = resp.read()
                    status = resp.status
                    ctype = resp.headers.get("Content-Type", "application/json")
            except HTTPError as e:
                data = e.read()
                status = e.code
                ctype = "application/json"
            except URLError as e:
                data = f'{{"error": "upstream unreachable: {e}"}}'.encode()
                status = 502
                ctype = "application/json"
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Connection", "close")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)
            self.close_connection = True

        def do_GET(self):
            self._forward("GET")

        def do_POST(self):
            self._forward("POST")

        def log_message(self, fmt, *args):
            pass

    return Handler


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def serve_port(port, path):
    ThreadingHTTPServer(("127.0.0.1", port), make_handler(path)).serve_forever()


if __name__ == "__main__":
    threads = [
        threading.Thread(target=serve_port, args=(port, path), daemon=True)
        for port, path in ROUTES.items()
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
