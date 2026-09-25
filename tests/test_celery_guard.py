"""KAN-1216: at-least-once delivery config + poison-task cap (src/celery_guard.py).

Tasks run through Celery's real trace path via eager ``.apply()`` (so the
custom ``__call__`` and ``after_return`` hooks fire as they do in a worker),
against an in-memory stand-in for Redis.
"""

import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

if isinstance(sys.modules.get("celery"), Mock):
    # Some workers' conftest stubs celery with a MagicMock for the whole run;
    # the guard needs the real trace path. This byte-identical file is
    # exercised in every worker that loads real celery.
    pytest.skip("celery is stubbed by this repo's conftest", allow_module_level=True)

from celery import Celery  # noqa: E402

from src import celery_guard  # noqa: E402
from src.celery_guard import (  # noqa: E402
    MAX_DELIVERIES,
    GuardedTask,
    PoisonTaskError,
    configure_delivery,
    delivery_key,
    record_delivery,
)


class FakePipeline:
    def __init__(self, store):
        self.store = store
        self.ops = []

    def incr(self, key):
        self.ops.append(("incr", key))
        return self

    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))
        return self

    def execute(self):
        out = []
        for op in self.ops:
            if op[0] == "incr":
                self.store.data[op[1]] = self.store.data.get(op[1], 0) + 1
                out.append(self.store.data[op[1]])
            else:
                self.store.ttls[op[1]] = op[2]
                out.append(True)
        return out


class FakeRedis:
    def __init__(self):
        self.data = {}
        self.ttls = {}

    def pipeline(self):
        return FakePipeline(self)

    def delete(self, key):
        self.data.pop(key, None)


class BrokenRedis:
    def pipeline(self):
        raise ConnectionError("redis down")

    def delete(self, key):
        raise ConnectionError("redis down")


@pytest.fixture
def fake_redis(monkeypatch):
    client = FakeRedis()
    monkeypatch.setattr(GuardedTask, "_guard_client", client)
    return client


@pytest.fixture
def app():
    app = Celery(broker="memory://", backend="cache+memory://", task_cls=GuardedTask)
    configure_delivery(app)
    app.conf.task_always_eager = False
    return app


def _make_task(app, body=lambda: "ok"):
    @app.task(bind=True, name="kan1216.test_task")
    def task(self):
        return body()

    return task


# -- configure_delivery ------------------------------------------------------


def test_configure_delivery_sets_at_least_once(app):
    assert app.conf.task_acks_late is True
    assert app.conf.task_reject_on_worker_lost is True
    assert app.conf.worker_prefetch_multiplier == 1


def test_visibility_timeout_is_12h(app):
    # KAN-933 / FINDING-16: must never drop to the 1h kombu default -- the
    # unacked hash is broker-global, so ONE low value duplicates long tasks
    # on every worker.
    assert app.conf.broker_transport_options["visibility_timeout"] == 43200


def test_configure_delivery_keeps_other_transport_options():
    app = Celery(broker="memory://", task_cls=GuardedTask)
    app.conf.broker_transport_options = {"max_retries": 7}
    configure_delivery(app)
    opts = app.conf.broker_transport_options
    assert opts["max_retries"] == 7
    assert opts["visibility_timeout"] == 43200


def test_unacked_bookkeeping_is_private(app):
    # Upstream workers on the same broker run kombu's 1h default against the
    # shared `unacked` hash; ours must live elsewhere or they restore (and
    # duplicate) our long tasks after an hour.
    opts = app.conf.broker_transport_options
    assert opts["unacked_key"] == "openrelik_unacked"
    assert opts["unacked_index_key"] == "openrelik_unacked_index"
    assert opts["unacked_mutex_key"] == "openrelik_unacked_mutex"
    assert "unacked" not in (opts["unacked_key"], opts["unacked_index_key"])


def test_configure_delivery_refuses_app_without_guard():
    # acks_late without the poison cap is the requeue-forever failure mode.
    with pytest.raises(RuntimeError, match="task_cls=GuardedTask"):
        configure_delivery(Celery(broker="memory://"))


def test_worker_app_is_wired():
    # Read, not imported: src/app.py pulls per-worker deps and env (REDIS_URL,
    # telemetry, ...), and this file is copied byte-identical to every worker.
    # configure_delivery() itself refuses an app built without GuardedTask.
    source = (Path(__file__).resolve().parents[1] / "src" / "app.py").read_text()
    assert "task_cls=GuardedTask" in source
    assert "configure_delivery(celery)" in source


# -- delivery counting -------------------------------------------------------


def test_record_delivery_counts_and_sets_ttl():
    client = FakeRedis()
    assert record_delivery(client, "abc", 0) == 1
    assert record_delivery(client, "abc", 0) == 2
    assert client.ttls[delivery_key("abc", 0)] == 43200 * 2


def test_retry_is_not_a_redelivery():
    # Celery retry() reuses the task id; each retry must count separately.
    client = FakeRedis()
    record_delivery(client, "abc", 0)
    assert record_delivery(client, "abc", 1) == 1


# -- GuardedTask -------------------------------------------------------------


def test_normal_task_runs_and_clears_its_count(app, fake_redis):
    task = _make_task(app)
    result = task.apply(task_id="t-ok")
    assert result.successful() and result.get() == "ok"
    # Returned normally -> count cleared; only worker-loss deliveries pile up.
    assert delivery_key("t-ok", 0) not in fake_redis.data


def test_failing_task_clears_count_too(app, fake_redis):
    def boom():
        raise ValueError("bad input")

    task = _make_task(app, boom)
    result = task.apply(task_id="t-fail")
    assert result.failed() and isinstance(result.result, ValueError)
    assert delivery_key("t-fail", 0) not in fake_redis.data


def test_redelivery_under_the_cap_still_runs(app, fake_redis):
    # Simulate MAX_DELIVERIES - 1 earlier deliveries whose worker was lost
    # (no after_return, so the count survived).
    fake_redis.data[delivery_key("t-redo", 0)] = MAX_DELIVERIES - 1
    ran = []
    task = _make_task(app, lambda: ran.append(1) or "ok")
    result = task.apply(task_id="t-redo")
    assert result.successful() and ran == [1]


def test_poison_task_fails_terminally_without_running(app, fake_redis, capsys):
    fake_redis.data[delivery_key("t-poison", 0)] = MAX_DELIVERIES
    ran = []
    task = _make_task(app, lambda: ran.append(1) or "ok")
    result = task.apply(task_id="t-poison")
    assert result.failed()
    assert isinstance(result.result, PoisonTaskError)
    assert ran == []  # body never executed -> no fourth OOM
    out = capsys.readouterr().out
    assert "poison_task t-poison" in out
    assert f"deliveries={MAX_DELIVERIES + 1}" in out
    assert f"max={MAX_DELIVERIES}" in out


def test_redis_outage_fails_open(app, monkeypatch, capsys):
    monkeypatch.setattr(GuardedTask, "_guard_client", BrokenRedis())
    task = _make_task(app)
    result = task.apply(task_id="t-noredis")
    assert result.successful() and result.get() == "ok"
    assert "delivery count unavailable" in capsys.readouterr().out


def test_direct_call_is_not_guarded(app, monkeypatch):
    # Calling the task function directly (as unit tests elsewhere do) has no
    # broker delivery and must not touch Redis.
    monkeypatch.setattr(GuardedTask, "_guard_client", BrokenRedis())
    task = _make_task(app)
    assert task() == "ok"


def test_module_defaults():
    assert celery_guard.MAX_DELIVERIES == 3
    assert celery_guard.VISIBILITY_TIMEOUT == 43200
