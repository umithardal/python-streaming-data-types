import time
import flatbuffers
from streaming_data_types.fbschemas.run_start_pl72 import RunStart
from streaming_data_types.utils import check_schema_identifier
from collections import namedtuple

FILE_IDENTIFIER = b"pl72"


def serialise_pl72(
    job_id,
    filename,
    start_time=None,
    stop_time=None,
    run_name="test_run",
    nexus_structure="{}",
    service_id="",
    instrument_name="TEST",
    broker="localhost:9092",
):
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
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    run_start = RunStart.RunStart.GetRootAsRunStart(buffer, 0)
    service_id = run_start.ServiceId() if run_start.ServiceId() else b""
    broker = run_start.Broker() if run_start.Broker() else b""
    job_id = run_start.JobId() if run_start.JobId() else b""
    filename = run_start.Filename() if run_start.Filename() else b""
    nexus_structure = run_start.NexusStructure() if run_start.NexusStructure() else b""
    instrument_name = run_start.InstrumentName() if run_start.InstrumentName() else b""
    run_name = run_start.RunName() if run_start.RunName() else b""

    RunStartInfo = namedtuple(
        "RunStartInfo",
        "job_id filename start_time stop_time run_name nexus_structure service_id instrument_name broker",
    )
    return RunStartInfo(
        job_id.decode(),
        filename.decode(),
        run_start.StartTime(),
        run_start.StopTime(),
        run_name.decode(),
        nexus_structure.decode(),
        service_id.decode(),
        instrument_name.decode(),
        broker.decode(),
    )
