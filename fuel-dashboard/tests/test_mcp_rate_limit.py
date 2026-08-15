import time

from mcp_layer.rate_limit import _FixedWindowLimiter
from mcp_layer.rate_limit import _parse_rate


def test_parse_rate_reads_count_and_period():
    assert _parse_rate("30/minute") == (30, 60)
    assert _parse_rate("1/second") == (1, 1)
    assert _parse_rate("5/hour") == (5, 3600)


def test_parse_rate_rejects_bad_format():
    try:
        _parse_rate("not-a-rate")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")


def test_allows_up_to_max_requests_then_blocks():
    limiter = _FixedWindowLimiter(max_requests=2, window_seconds=60)
    assert limiter.allow("ip1") is True
    assert limiter.allow("ip1") is True
    assert limiter.allow("ip1") is False


def test_resets_after_window_elapses():
    limiter = _FixedWindowLimiter(max_requests=1, window_seconds=0.05)
    assert limiter.allow("ip1") is True
    assert limiter.allow("ip1") is False
    time.sleep(0.06)
    assert limiter.allow("ip1") is True


def test_buckets_are_independent_per_key():
    limiter = _FixedWindowLimiter(max_requests=1, window_seconds=60)
    assert limiter.allow("ip1") is True
    assert limiter.allow("ip2") is True
    assert limiter.allow("ip1") is False
    assert limiter.allow("ip2") is False


def test_prunes_stale_buckets_to_bound_memory():
    # Regression test: _buckets used to grow forever, one entry per distinct client IP ever seen
    # (bots/scanners included) on a long-running, resource-constrained deployment.
    limiter = _FixedWindowLimiter(max_requests=100, window_seconds=60)
    now = time.monotonic()
    limiter._buckets = {f"stale-{i}": (now - 1000, 5) for i in range(50)}
    limiter._last_prune = now - limiter._prune_interval - 1  # force the next allow() to sweep

    limiter.allow("fresh-ip")

    assert set(limiter._buckets) == {"fresh-ip"}


def test_prune_does_not_evict_live_buckets():
    limiter = _FixedWindowLimiter(max_requests=100, window_seconds=60)
    limiter.allow("live-ip")
    now = time.monotonic()
    limiter._last_prune = now - limiter._prune_interval - 1  # force the next allow() to sweep

    limiter.allow("live-ip")
    limiter.allow("another-ip")

    assert {"live-ip", "another-ip"} == set(limiter._buckets)
