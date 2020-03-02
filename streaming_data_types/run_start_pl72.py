import time
from typing import Optional
import flatbuffers
from streaming_data_types.fbschemas.run_start_pl72 import RunStart
from streaming_data_types.utils import get_schema


FILE_IDENTIFIER = b"pl72"


def serialise_pl72(
    job_id: str,
    filename: str,
    start_time: Optional[int] = None,
    stop_time: Optional[int] = None,
    run_name: str = "test_run",
    nexus_structure: str = "{}",
    service_id: str = "",
    instrument_name: str = "TEST",
    broker: str = "localhost:9092",
) -> bytes:
    builder = flatbuffers.Builder(136)

    if start_time is None:
        start_time = int(time.time() * 1000)
    if service_id is None:
        service_id = ""
    if stop_time is None:
        stop_time = 0

    service_id_offset = builder.CreateString(service_id)
    broker_offset = builder.CreateString(broker)
    job_id_offset = builder.CreateString(job_id)
    nexus_structure_offset = builder.CreateString(nexus_structure)
    instrument_name_offset = builder.CreateString(instrument_name)
    run_name_offset = builder.CreateString(run_name)
    filename_offset = builder.CreateString(filename)

    # Build the actual buffer
    RunStart.RunStartStart(builder)
    RunStart.RunStartAddServiceId(builder, service_id_offset)
    RunStart.RunStartAddBroker(builder, broker_offset)
    RunStart.RunStartAddJobId(builder, job_id_offset)
    RunStart.RunStartAddNexusStructure(builder, nexus_structure_offset)
    RunStart.RunStartAddInstrumentName(builder, instrument_name_offset)
    RunStart.RunStartAddRunName(builder, run_name_offset)
    RunStart.RunStartAddStopTime(builder, stop_time)
    RunStart.RunStartAddStartTime(builder, start_time)
    RunStart.RunStartAddFilename(builder, filename_offset)
    RunStart.RunStartAddNPeriods(builder, 1)

    run_start_message = RunStart.RunStartEnd(builder)
    builder.Finish(run_start_message)

    # Generate the output and replace the file_identifier
    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER
    return bytes(buffer)


def deserialise_pl72(buffer):
    # Check schema is correct
    if get_schema(buffer) != FILE_IDENTIFIER.decode():
        raise RuntimeError(
            f"Incorrect schema: expected {FILE_IDENTIFIER} but got "
            f"{get_schema(buffer)}"
        )

    # run_start = RunStart.RunStart.GetRootAsRunStart(buffer, 0)
    # service_id = run_start.ServiceId() if run_start.ServiceId() else b""
    # broker = run_start.Broker() if run_start.Broker() else b""
    # job_id =

    # TODO: return a namedTuple like ns10?
