"""Migration plan Phase 4: Scheduler -- direct translation of cttc's own
scheduler.py.
"""

import pytest
from fakeredis import FakeAsyncRedis
from log_sump_server.scheduling import InvalidSchedule, Scheduler, UnknownSchedule
from log_sump_server.sessions import SessionManager

DOCKER_HOST = "daemon-a"


def _scheduler() -> Scheduler:
    return Scheduler(SessionManager(FakeAsyncRedis()))


def test_create_requires_exactly_one_of_start_at_or_cron() -> None:
    scheduler = _scheduler()
    with pytest.raises(InvalidSchedule):
        scheduler.create(DOCKER_HOST, duration_minutes=5.0)
    with pytest.raises(InvalidSchedule):
        scheduler.create(DOCKER_HOST, duration_minutes=5.0, start_at=1000.0, cron="* * * * *")


def test_create_rejects_invalid_cron() -> None:
    scheduler = _scheduler()
    with pytest.raises(InvalidSchedule):
        scheduler.create(DOCKER_HOST, duration_minutes=5.0, cron="not a cron expression")


def test_cancel_unknown_schedule_raises() -> None:
    scheduler = _scheduler()
    with pytest.raises(UnknownSchedule):
        scheduler.cancel("sch999")


def test_one_shot_fires_once_at_start_at() -> None:
    scheduler = _scheduler()
    sid = scheduler.create(DOCKER_HOST, duration_minutes=5.0, start_at=1_000_000.0)

    scheduler.tick(now=999_999.0)
    assert scheduler.status_of(sid)["done"] is False
    assert scheduler.status_of(sid)["session_ids"] == []

    scheduler.tick(now=1_000_001.0)
    status = scheduler.status_of(sid)
    assert status["done"] is True
    assert len(status["session_ids"]) == 1

    scheduler.tick(now=2_000_000.0)  # must not fire a second time
    assert len(scheduler.status_of(sid)["session_ids"]) == 1


def test_recurring_fires_once_per_tick_even_after_missed_occurrences() -> None:
    scheduler = _scheduler()
    sid = scheduler.create(DOCKER_HOST, duration_minutes=1.0, cron="* * * * *")
    initial_next_fire = scheduler._schedules[sid].next_fire
    assert initial_next_fire is not None

    # Simulate a huge gap (e.g. a suspended process) -- many minutes' worth
    # of "* * * * *" occurrences have been missed by the time of this tick.
    now = initial_next_fire + 60 * 60_000.0  # +1 hour

    scheduler.tick(now=now)

    status = scheduler.status_of(sid)
    assert len(status["session_ids"]) == 1  # fired exactly once, not ~60 times
    new_next_fire = scheduler._schedules[sid].next_fire
    assert new_next_fire is not None
    assert new_next_fire > now - 60_000.0  # fast-forwarded to near "now"


def test_cancel_removes_schedule() -> None:
    scheduler = _scheduler()
    sid = scheduler.create(DOCKER_HOST, duration_minutes=5.0, start_at=1_000_000.0)

    scheduler.cancel(sid)

    with pytest.raises(UnknownSchedule):
        scheduler.status_of(sid)
