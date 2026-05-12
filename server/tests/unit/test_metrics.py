import pytest

pytest.importorskip("prometheus_client")

from prometheus_client import CONTENT_TYPE_LATEST  # noqa: E402
from twisted.web.test.requesthelper import DummyRequest  # noqa: E402

from recceiver import metrics  # noqa: E402


class TestMetricsAvailable:
    def test_available_flag_is_true(self):
        assert metrics.available is True

    def test_make_site_returns_twisted_site(self):
        from twisted.web.server import Site

        assert isinstance(metrics.make_site(), Site)


class TestMetricsEndpoint:
    def test_render_get_sets_prometheus_content_type(self):
        request = DummyRequest([b"/metrics"])
        metrics._MetricsResource().render_GET(request)
        assert request.responseHeaders.getRawHeaders(b"Content-Type") == [CONTENT_TYPE_LATEST.encode()]

    def test_render_get_returns_expected_metric_names(self):
        request = DummyRequest([b"/metrics"])
        body = metrics._MetricsResource().render_GET(request)
        assert isinstance(body, bytes)
        for name in (
            b"recceiver_connections_active",
            b"recceiver_connections_waiting",
            b"recceiver_connections_limit",
            b"recceiver_known_iocs",
            b"recceiver_tracked_channels",
            b"recceiver_cf_commits_total",
            b"recceiver_cf_commit_duration_seconds",
        ):
            assert name in body, f"{name!r} not found in metrics output"
