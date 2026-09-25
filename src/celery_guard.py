# Copyright 2026 CYPFER
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""KAN-1216: at-least-once task delivery, with a poison-task cap.

This file is copied BYTE-IDENTICAL into every openrelik fork worker. Edit it
here, then re-copy it everywhere; do not let the copies drift.

Why
---
Celery's default ``acks_late=False`` acks a message *before* the task runs, so
a worker killed mid-task (OOM, ts-worker-watchdog restart, disk_guard,
container recreate, operator bounce) loses the task silently. The broker has
nothing left to redeliver, and reconcile cannot heal a task that never
finished (KAN-1184). ``configure_delivery`` moves the ack to *after* the task
and rejects-with-requeue when the pool child dies.

How that behaves on the Redis transport (kombu 5.x, read from source):

* Pool child killed (the kernel OOM killer picks the big child, not the
  parent): ``WorkerLostError`` -> ``reject(requeue=True)`` -> the message is
  pushed back onto the queue at once.
* Whole worker killed (``docker kill``, SIGKILL after the stop grace period):
  the message stays in the broker's GLOBAL ``unacked`` hash. Whichever
  consumer on the broker next runs ``restore_visible`` puts it back once it is
  older than THAT consumer's ``visibility_timeout``.

That second point is why ``visibility_timeout`` is set here and not left to
each worker. The ``unacked`` hash is shared by every consumer of the broker, so
the effective redelivery bound is the SMALLEST timeout any worker uses. With
``acks_late`` a running task stays unacked for its whole run, so one worker
left on the 1h default would restore, and duplicate, every task running longer
than an hour on every other worker. 12h matches plaso's FINDING-16 / KAN-933
setting and must not be lowered. For the same reason prefetch drops to 1: a
prefetched message waiting behind a busy lane is also unacked and would also
be restored.

Setting 12h on OUR workers is not enough on its own. A case broker also
serves upstream / third-party workers we do not build (capa, yara,
bulkextractor, eztools, kstrike, volatility, ...), all on kombu's 1h default.
Each would scan the shared ``unacked`` hash and restore our long tasks after
an hour. So our fleet parks its in-flight messages under its OWN keys
(``UNACKED_KEY`` and friends), which a default consumer never scans. Every
worker that uses this module must use the same keys, and it does, because the
file is copied byte-identical.

Poison cap
----------
Redelivery on worker loss means a task that ALWAYS kills its worker (the
case-2264 / case-2471 OOM shape) would requeue forever. ``GuardedTask`` counts
each delivery in Redis, keyed by task id + retry number (so a Celery
``retry()``, which reuses the task id, is not mistaken for a redelivery). Once a
task reaches ``MAX_DELIVERIES`` it fails terminally instead of running. The
worker logs a greppable ``poison_task`` line for the alert layer (KAN-1217);
it pairs with the mediator's ``orphaned_task``.
"""

import os

import redis
from celery import Task

VISIBILITY_TIMEOUT = int(os.getenv("OPENRELIK_BROKER_VISIBILITY_TIMEOUT_SEC", "43200"))
# Total deliveries allowed: the first run plus (MAX_DELIVERIES - 1)
# redeliveries after worker loss. The next delivery fails terminally.
MAX_DELIVERIES = int(os.getenv("OPENRELIK_TASK_MAX_DELIVERIES", "3"))
DELIVERY_KEY_PREFIX = "openrelik:delivery-count:"
# Our fleet's private unacked bookkeeping (kombu defaults: unacked,
# unacked_index, unacked_mutex). See "Setting 12h on OUR workers" above.
UNACKED_KEY = "openrelik_unacked"
UNACKED_INDEX_KEY = "openrelik_unacked_index"
UNACKED_MUTEX_KEY = "openrelik_unacked_mutex"
# Outlive the gap between two deliveries. A whole-worker kill redelivers only
# after visibility_timeout, so a shorter TTL would reset the count every time.
DELIVERY_KEY_TTL = VISIBILITY_TIMEOUT * 2


class PoisonTaskError(Exception):
    """A task was delivered MAX_DELIVERIES times without ever returning."""


def delivery_key(task_id, retries):
    return f"{DELIVERY_KEY_PREFIX}{task_id}:{retries or 0}"


def record_delivery(client, task_id, retries, ttl=DELIVERY_KEY_TTL):
    """Count one delivery of this task attempt; return the running total."""
    key = delivery_key(task_id, retries)
    pipe = client.pipeline()
    pipe.incr(key)
    pipe.expire(key, ttl)
    count, _ = pipe.execute()
    return int(count)


def _log(message):
    # print, not a logger: several workers disable Celery's root-logger hijack
    # and configure their own, but stdout always reaches `docker logs` / Loki.
    print(f"celery_guard: {message}", flush=True)


class GuardedTask(Task):
    """Task base that fails terminally instead of running a poison task."""

    _guard_client = None

    def _guard_redis(self):
        if GuardedTask._guard_client is None:
            GuardedTask._guard_client = redis.Redis.from_url(self.app.conf.broker_url)
        return GuardedTask._guard_client

    def __call__(self, *args, **kwargs):
        request = self.request
        if request.id and not request.called_directly:
            try:
                deliveries = record_delivery(
                    self._guard_redis(), request.id, request.retries)
            except Exception as exc:  # noqa: BLE001
                # Fail open. A Redis hiccup must never stop real work; the cap
                # is a backstop, not a gate.
                _log(f"delivery count unavailable for {request.id} "
                     f"task={self.name}: {type(exc).__name__}: {exc}")
                deliveries = 0
            if deliveries > MAX_DELIVERIES:
                _log(
                    f"poison_task {request.id} task={self.name} "
                    f"deliveries={deliveries} max={MAX_DELIVERIES} -- delivered "
                    f"{deliveries} times without returning (worker lost each "
                    f"time); failing terminally instead of requeueing again."
                )
                raise PoisonTaskError(
                    f"task {request.id} ({self.name}) was delivered "
                    f"{deliveries} times without completing; the worker was "
                    f"lost every time (likely OOM). Failing terminally to stop "
                    f"a requeue loop (KAN-1216)."
                )
        return super().__call__(*args, **kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        # Any return (success OR failure) means the worker was not lost, so
        # clear the count. Only consecutive worker-loss deliveries accumulate.
        request = self.request
        if task_id and not request.called_directly:
            try:
                self._guard_redis().delete(delivery_key(task_id, request.retries))
            except Exception:  # noqa: BLE001
                pass
        super().after_return(status, retval, task_id, args, kwargs, einfo)


def configure_delivery(app):
    """Switch a worker's Celery app to at-least-once delivery.

    ``app`` must have been built with ``task_cls=GuardedTask``, because the
    task base class is fixed when the app is constructed. This checks that
    rather than let a worker ship requeue-on-loss without the poison cap.
    """
    if not issubclass(app.Task, GuardedTask):
        raise RuntimeError(
            "KAN-1216: build the Celery app with task_cls=GuardedTask before "
            "calling configure_delivery(); acks_late without the poison cap "
            "can requeue an OOM task forever."
        )
    app.conf.task_acks_late = True
    app.conf.task_reject_on_worker_lost = True
    app.conf.worker_prefetch_multiplier = 1
    options = dict(app.conf.broker_transport_options or {})
    options["visibility_timeout"] = VISIBILITY_TIMEOUT
    options["unacked_key"] = UNACKED_KEY
    options["unacked_index_key"] = UNACKED_INDEX_KEY
    options["unacked_mutex_key"] = UNACKED_MUTEX_KEY
    app.conf.broker_transport_options = options
