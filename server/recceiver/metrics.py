# -*- coding: utf-8 -*-

import logging

_log = logging.getLogger(__name__)

try:
    from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram, generate_latest
    from twisted.web.resource import Resource
    from twisted.web.server import Site

    _registry = CollectorRegistry(auto_describe=True)

    connections_active = Gauge(
        "recceiver_connections_active",
        "Active uploading IOC connections",
        registry=_registry,
    )
    connections_waiting = Gauge(
        "recceiver_connections_waiting",
        "IOC connections waiting for an upload slot",
        registry=_registry,
    )
    connections_limit = Gauge(
        "recceiver_connections_limit",
        "Maximum concurrent active connections (maxActive)",
        registry=_registry,
    )
    known_iocs = Gauge(
        "recceiver_known_iocs",
        "IOCs with channels currently registered in CF",
        registry=_registry,
    )
    tracked_channels = Gauge(
        "recceiver_tracked_channels",
        "Unique channel names tracked by the CF processor",
        registry=_registry,
    )
    cf_commits_total = Counter(
        "recceiver_cf_commits_total",
        "CF push attempts by result",
        ["result"],
        registry=_registry,
    )
    cf_commit_duration_seconds = Histogram(
        "recceiver_cf_commit_duration_seconds",
        "CF push duration in seconds",
        buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
        registry=_registry,
    )

    class _MetricsResource(Resource):
        isLeaf = True

        def render_GET(self, request):
            request.setHeader(b"Content-Type", CONTENT_TYPE_LATEST.encode())
            return generate_latest(_registry)

    def make_site():
        return Site(_MetricsResource())

    available = True

except ImportError:
    available = False

    class _Noop:
        def set(self, v=None):
            pass  # no-op: prometheus_client not installed

        def inc(self, v=1):
            pass  # no-op: prometheus_client not installed

        def labels(self, **_kw):
            return self

        def observe(self, v):
            pass  # no-op: prometheus_client not installed

    connections_active = _Noop()
    connections_waiting = _Noop()
    connections_limit = _Noop()
    known_iocs = _Noop()
    tracked_channels = _Noop()
    cf_commits_total = _Noop()
    cf_commit_duration_seconds = _Noop()

    def make_site():
        raise RuntimeError("prometheus_client is not installed")
