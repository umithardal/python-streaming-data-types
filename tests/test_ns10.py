from streaming_data_types.ns10 import serialise_ns10, deserialise_ns10


class TestSerialisationNs10:
    def test_serialises_and_deserialises_ns10_message_correctly(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_entry = {
            "key": "some_key",
            "time": 123456,
            "ttl": 567890,
            "expired": True,
            "value": "some_value",
        }

        buf = serialise_ns10(original_entry)
        entry = deserialise_ns10(buf)

        assert entry["key"] == original_entry["key"]
        assert entry["time"] == original_entry["time"]
        assert entry["ttl"] == original_entry["ttl"]
        assert entry["expired"] == original_entry["expired"]
        assert entry["value"] == original_entry["value"]
