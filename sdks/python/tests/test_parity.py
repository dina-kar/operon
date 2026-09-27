"""Rule 9: `Client` and `AsyncClient` expose the same surface."""

from __future__ import annotations

import inspect

import pytest

import operon

PAIRS = [
    (operon.Client, operon.AsyncClient),
    (operon.Namespace, operon.AsyncNamespace),
]


def _public(cls: type) -> list[str]:
    return sorted(name for name in vars(cls) if not name.startswith("_"))


def _params(func: object) -> list[tuple[str, object, object]]:
    signature = inspect.signature(func)  # type: ignore[arg-type]
    return [(p.name, p.kind, p.default) for p in signature.parameters.values()]


@pytest.mark.parametrize(("sync", "asynchronous"), PAIRS)
def test_sync_and_async_surfaces_match(sync: type, asynchronous: type) -> None:
    assert _public(sync) == _public(asynchronous)
    for name in ["__init__", *_public(sync)]:
        a, b = getattr(sync, name), getattr(asynchronous, name)
        if not callable(a):
            continue
        assert _params(a) == _params(b), name
        if name != "__init__" and inspect.iscoroutinefunction(b):
            assert not inspect.iscoroutinefunction(a), name


def test_async_methods_that_make_requests_are_coroutines() -> None:
    assert inspect.iscoroutinefunction(operon.AsyncClient.create_namespace)
    assert inspect.iscoroutinefunction(operon.AsyncClient.close)
    assert not inspect.iscoroutinefunction(operon.AsyncClient.namespace)
    for name in ["create_stream", "get_stream", "produce", "fetch"]:
        assert inspect.iscoroutinefunction(getattr(operon.AsyncNamespace, name)), name


def test_importing_operon_imports_no_extra() -> None:
    import subprocess
    import sys

    code = (
        "import sys, operon; "
        "bad = [m for m in ('pyarrow', 'polars', 'adbc_driver_manager', 'adbc_driver_flightsql') "
        "if m in sys.modules]; print(bad); sys.exit(1 if bad else 0)"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
