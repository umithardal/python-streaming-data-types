import pytest
from streaming_data_types.logdata_f142 import serialise_f142, deserialise_f142


class TestSerialisationf142:
    original_entry = {
        "source_name": "some_source",
        "value": 578214,
        "timestamp_unix_ns": 1585332414000000000,
    }

    def test_serialises_and_deserialises_f142_message_correctly(self):
        buf = serialise_f142(**self.original_entry)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == self.original_entry["source_name"]
        assert deserialised_tuple.value == self.original_entry["value"]
        assert (
            deserialised_tuple.timestamp_unix_ns
            == self.original_entry["timestamp_unix_ns"]
        )

    def test_serialises_and_deserialises_string_f142_message_correctly(self):
        string_log = {
            "source_name": "some_source",
            "value": "some_string",
            "timestamp_unix_ns": 1585332414000000000,
        }
        buf = serialise_f142(**string_log)
        deserialised_tuple = deserialise_f142(buf)

        assert deserialised_tuple.source_name == string_log["source_name"]
        assert deserialised_tuple.value == string_log["value"]
        assert deserialised_tuple.timestamp_unix_ns == string_log["timestamp_unix_ns"]

    def test_if_buffer_has_wrong_id_then_throws(self):
        buf = serialise_f142(**self.original_entry)

        # Manually hack the id
        buf[4:8] = b"1234"

        with pytest.raises(RuntimeError):
            deserialise_f142(buf)
