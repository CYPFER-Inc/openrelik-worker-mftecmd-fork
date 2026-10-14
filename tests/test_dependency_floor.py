"""KAN-1422: the image must never resolve a PyJWT affected by the 2.13.x/2.14.0
advisories. PyJWT is transitive (redis>=5.1 requires it, via celery[redis] /
openrelik-worker-common), so guard the direct floor in pyproject.toml and,
when a lock file is committed, the version it pins.

Same pattern as openrelik-pipeline cleanup-worker/tests/test_dependency_floor.py
(KAN-1410).
"""
from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent
_FLOOR = (2, 15, 0)


def _ver(s: str) -> tuple[int, ...]:
    return tuple(int(p) for p in re.findall(r"\d+", s)[:3])


def _pyjwt_spec() -> str | None:
    data = tomllib.loads((_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    poetry = data.get("tool", {}).get("poetry", {}).get("dependencies", {})
    for name, spec in poetry.items():
        if name.lower() == "pyjwt":
            return spec if isinstance(spec, str) else spec.get("version")
    for dep in data.get("project", {}).get("dependencies", []):
        m = re.match(r"\s*pyjwt\s*(?:\[[^\]]*\])?\s*(.*)$", dep, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def test_pyproject_declares_pyjwt_floor():
    spec = _pyjwt_spec()
    assert spec is not None, "direct pyjwt floor missing from pyproject.toml"
    m = re.search(r">=\s*([0-9][0-9.]*)", spec)
    assert m, f"pyjwt spec has no >= floor: {spec!r}"
    assert _ver(m.group(1)) >= _FLOOR, spec


@pytest.mark.parametrize("lock_name", ["poetry.lock", "uv.lock"])
def test_lock_pins_patched_pyjwt(lock_name):
    lock_path = _ROOT / lock_name
    if not lock_path.exists():
        pytest.skip(f"no {lock_name} committed")
    lock = tomllib.loads(lock_path.read_text(encoding="utf-8"))
    pins = {p["name"].lower(): p["version"] for p in lock.get("package", [])}
    assert "pyjwt" in pins, f"{lock_name} has no pyjwt entry"
    assert _ver(pins["pyjwt"]) >= _FLOOR, pins["pyjwt"]
