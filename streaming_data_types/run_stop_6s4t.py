from typing import Optional
import flatbuffers
from streaming_data_types.fbschemas.run_stop_6s4t import RunStop


def serialise_6s4t(
    job_id: str,
    run_name: str = "test_run",
    service_id: str = "",
    stop_time: Optional[int] = None,
) -> bytes:
    builder = flatbuffers.Builder(136)

    if service_id is None:
        service_id = ""
    if stop_time is None:
        stop_time = 0

    service_id_offset = builder.CreateString(service_id)
    job_id_offset = builder.CreateString(job_id)
    run_name_offset = builder.CreateString(run_name)

    # Build the actual buffer
    RunStop.RunStopStart(builder)
    RunStop.RunStopAddServiceId(builder, service_id_offset)
    RunStop.RunStopAddJobId(builder, job_id_offset)
    RunStop.RunStopAddRunName(builder, run_name_offset)
    RunStop.RunStopAddStopTime(builder, stop_time)

    run_stop_message = RunStop.RunStopEnd(builder)
    builder.Finish(run_stop_message)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = b"6s4t"
    return bytes(buff)
