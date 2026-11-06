"""Minimal Prometheus text-format metrics (no client library needed)."""

from __future__ import annotations

import threading
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple], float] = defaultdict(float)
        self._gauges: dict[tuple[str, tuple], float] = {}
        self._help: dict[str, tuple[str, str]] = {}

    def inc(self, name: str, value: float = 1.0, help: str = "", **labels: str) -> None:
        with self._lock:
            self._help.setdefault(name, ("counter", help))
            self._counters[(name, tuple(sorted(labels.items())))] += value

    def set(self, name: str, value: float, help: str = "", **labels: str) -> None:
        with self._lock:
            self._help.setdefault(name, ("gauge", help))
            self._gauges[(name, tuple(sorted(labels.items())))] = value

    def get(self, name: str, **labels: str) -> float:
        key = (name, tuple(sorted(labels.items())))
        return self._counters.get(key, self._gauges.get(key, 0.0))

    def render(self) -> str:
        lines = []
        with self._lock:
            series = {**self._counters, **self._gauges}
            for name in sorted(self._help):
                kind, help_text = self._help[name]
                lines.append(f"# HELP {name} {help_text}")
                lines.append(f"# TYPE {name} {kind}")
                for (n, labels), v in sorted(series.items()):
                    if n != name:
                        continue
                    lbl = ",".join(f'{k}="{val}"' for k, val in labels)
                    lines.append(f"{name}{{{lbl}}} {v}" if lbl else f"{name} {v}")
        return "\n".join(lines) + "\n"


metrics = Metrics()


def serve_metrics(port: int, registry: Metrics = metrics) -> ThreadingHTTPServer:
    """Expose /metrics from a background thread (for worker processes that have no web framework)."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = registry.render().encode() if self.path == "/metrics" else b"ok\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):  # keep logs quiet
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
