import pytest
from streaming_data_types.run_stop_6s4t import serialise_6s4t, deserialise_6s4t


class TestSerialisation6s4t:
    original_entry = {
        "job_id": "some_key",
        "stop_time": 578214,
        "run_name": "test_run",
        "service_id": "filewriter1",
    }

    def test_serialises_and_deserialises_6s4t_message_correctly(self):
        buf = serialise_6s4t(**self.original_entry)
        deserialised_tuple = deserialise_6s4t(buf)

        assert deserialised_tuple.job_id == self.original_entry["job_id"]
        assert deserialised_tuple.stop_time == self.original_entry["stop_time"]
        assert deserialised_tuple.run_name == self.original_entry["run_name"]
        assert deserialised_tuple.service_id == self.original_entry["service_id"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_6s4t(**self.original_entry)

        # Manually hack the id
        buf[4:8] = b"1234"

        with pytest.raises(RuntimeError):
            deserialise_6s4t(buf)