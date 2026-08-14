import asyncio
import itertools

from log_sump_common.schema import RecordAdapter, ServiceRecord
from log_sump_common.transport import ExecResult, TransportError
from log_sump_listener.services_listing import _sample_once, run_services_listing

from .conftest import FakeRecordsLogger, FakeTransport

SERVICE_LS_OUTPUT = (
    '{"ID":"s1abc","Name":"web","Replicas":"3/3"}\n{"ID":"s2xyz","Name":"db","Replicas":"1/1"}\n'
)


async def test_sample_once_ships_one_record_per_service() -> None:
    result = ExecResult(returncode=0, stdout=SERVICE_LS_OUTPUT, stderr="")
    transport = FakeTransport(run_result=result)
    records_logger = FakeRecordsLogger()
    seq = itertools.count(1)

    await _sample_once("daemon-a", transport, records_logger, seq)

    assert len(records_logger.calls) == 2
    records = [RecordAdapter.validate_json(c) for c in records_logger.calls]
    assert all(isinstance(r, ServiceRecord) for r in records)
    names = {r.name for r in records if isinstance(r, ServiceRecord)}
    assert names == {"web", "db"}


async def test_run_services_listing_tolerates_not_a_swarm_manager() -> None:
    transport = FakeTransport(raise_on_start=TransportError("this node is not a swarm manager"))
    records_logger = FakeRecordsLogger()

    task = asyncio.create_task(
        run_services_listing("daemon-a", transport, records_logger, listing_interval_s=10.0)
    )
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert records_logger.calls == []  # survived, shipped nothing -- not a fatal error
