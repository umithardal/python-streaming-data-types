# from .context import streaming_data_types  # NOQA
import numpy as np
from streaming_data_types.hs00 import serialise_hs00, deserialise_hs00


class TestSerialisationHs00:
    def test_serialises_and_deserialises_hs00_message_correctly_for_full_1d_data(self):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_hist = {
            "source": "some_source",
            "timestamp": 123456,
            "current_shape": [5],
            "dim_metadata": [
                {
                    "length": 5,
                    "unit": "m",
                    "label": "some_label",
                    "bin_boundaries": [0, 1, 2, 3, 4, 5],
                }
            ],
            "last_metadata_timestamp": 123456,
            "data": [1, 2, 3, 4, 5],
            "errors": [5, 4, 3, 2, 1],
            "info": "info_string",
        }
        buf = serialise_hs00(original_hist)

        hist = deserialise_hs00(buf)
        assert hist["source"] == original_hist["source"]
        assert hist["timestamp"] == original_hist["timestamp"]
        assert hist["shape"] == original_hist["current_shape"]
        assert (
            hist["dims"][0]["edges"]
            == original_hist["dim_metadata"][0]["bin_boundaries"]
        )
        assert hist["dims"][0]["length"] == original_hist["dim_metadata"][0]["length"]
        assert hist["dims"][0]["unit"] == original_hist["dim_metadata"][0]["unit"]
        assert hist["dims"][0]["label"] == original_hist["dim_metadata"][0]["label"]
        assert np.array_equal(hist["data"], original_hist["data"])
        assert np.array_equal(hist["errors"], original_hist["errors"])
        assert hist["info"] == original_hist["info"]
        assert (
            hist["last_metadata_timestamp"] == original_hist["last_metadata_timestamp"]
        )

    def test_serialises_and_deserialises_hs00_message_correctly_for_minimal_1d_data(
        self
    ):
        """
        Round-trip to check what we serialise is what we get back.
        """
        original_hist = {
            "timestamp": 123456,
            "current_shape": [5],
            "dim_metadata": [
                {
                    "length": 5,
                    "unit": "m",
                    "label": "some_label",
                    "bin_boundaries": [0, 1, 2, 3, 4, 5],
                }
            ],
            "data": [1, 2, 3, 4, 5],
        }
        buf = serialise_hs00(original_hist)

        hist = deserialise_hs00(buf)
        assert hist["source"] == ""
        assert hist["timestamp"] == original_hist["timestamp"]
        assert hist["shape"] == original_hist["current_shape"]
        assert (
            hist["dims"][0]["edges"]
            == original_hist["dim_metadata"][0]["bin_boundaries"]
        )
        assert hist["dims"][0]["length"] == original_hist["dim_metadata"][0]["length"]
        assert hist["dims"][0]["unit"] == original_hist["dim_metadata"][0]["unit"]
        assert hist["dims"][0]["label"] == original_hist["dim_metadata"][0]["label"]
        assert np.array_equal(hist["data"], original_hist["data"])
        assert len(hist["errors"]) == 0
        assert hist["info"] == ""


# TODO: test with non-required fields missing, M-D data
