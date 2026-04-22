"""Pytest config for oj-persistence's unit suite.

Adds two autouse fixtures:

- ``_reset_backend_registry``: each test gets a fresh process-global
  ``_BACKENDS`` registry so backend reference counting doesn't leak.
- ``_strict_visibility``: fails any test that silently emits an ERROR
  log, dies in a background thread, or leaves an unawaited failed task.
  Opt out with ``@pytest.mark.expect_error``.
"""

import asyncio

import pytest

from oj_persistence import manager as manager_module
from oj_toolkit.diagnostics import strict_visibility


@pytest.fixture(autouse=True)
def _reset_backend_registry():
    """Clear the process-global backend registry around each test."""
    yield
    with manager_module._BACKENDS._lock:
        backends = list(manager_module._BACKENDS._backends.values())
        manager_module._BACKENDS._backends.clear()
        manager_module._BACKENDS._refcounts.clear()
    for backend in backends:
        try:
            asyncio.new_event_loop().run_until_complete(backend.aclose())
        except Exception:
            pass


@pytest.fixture(autouse=True)
def _strict_visibility(request):
    """Fail tests that emit ERROR logs or leak async/thread failures.

    Opt out per-test with ``@pytest.mark.expect_error``.
    """
    if request.node.get_closest_marker('expect_error') is not None:
        yield
        return
    with strict_visibility():
        yield


def pytest_configure(config):
    config.addinivalue_line('markers', 'expect_error: suppress strict_visibility for this test')
