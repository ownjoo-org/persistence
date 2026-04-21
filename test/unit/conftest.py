"""Pytest config for the v2 proposal test suite."""

import pytest

from oj_persistence import manager as manager_module


@pytest.fixture(autouse=True)
def _reset_backend_registry():
    """Each test gets a fresh process-global backend registry.

    The production contract is "one Backend per file process-wide," but
    test isolation needs every test to start clean. Snapshot before, clear
    after.
    """
    yield
    # Close any stragglers so tests don't leak connections
    with manager_module._BACKENDS._lock:
        backends = list(manager_module._BACKENDS._backends.values())
        manager_module._BACKENDS._backends.clear()
        manager_module._BACKENDS._refcounts.clear()
    import asyncio
    for backend in backends:
        try:
            asyncio.get_event_loop().run_until_complete(backend.aclose())
        except Exception:
            pass
