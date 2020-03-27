import flatbuffers
from streaming_data_types.fbschemas.logdata_f142 import LogData
from streaming_data_types.fbschemas.logdata_f142.Value import Value
from streaming_data_types.fbschemas.logdata_f142.UByte import (
    UByteStart,
    UByteAddValue,
    UByteEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Byte import (
    ByteStart,
    ByteAddValue,
    ByteEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Short import (
    ShortStart,
    ShortAddValue,
    ShortEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Int import (
    IntStart,
    IntAddValue,
    IntEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Long import (
    LongStart,
    LongAddValue,
    LongEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Float import (
    FloatStart,
    FloatAddValue,
    FloatEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Double import (
    DoubleStart,
    DoubleAddValue,
    DoubleEnd,
)
from streaming_data_types.fbschemas.logdata_f142.String import (
    StringStart,
    StringAddValue,
    StringEnd,
)
import numpy as np
from typing import Any


def _complete_buffer(builder, timestamp_unix_ns: int) -> bytearray:
    LogData.LogDataAddTimestamp(builder, timestamp_unix_ns)
    log_msg = LogData.LogDataEnd(builder)
    builder.Finish(log_msg)
    buff = builder.Output()
    file_identifier = b"f142"
    buff[4:8] = file_identifier
    return buff


def _setup_builder():
    builder = flatbuffers.Builder(1024)
    source = builder.CreateString("Forwarder-Python")
    return builder, source


def _serialise_byte(builder, data, source):
    ByteStart(builder)
    ByteAddValue(builder, data.astype(np.byte)[0])
    value_position = ByteEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Byte)


def _serialise_ubyte(builder, data, source):
    UByteStart(builder)
    UByteAddValue(builder, data.astype(np.ubyte)[0])
    value_position = UByteEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.UByte)


def _serialise_short(builder, data, source):
    ShortStart(builder)
    ShortAddValue(builder, data.astype(np.int16)[0])
    value_position = ShortEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Short)


def _serialise_int(builder, data, source):
    IntStart(builder)
    IntAddValue(builder, data.astype(np.int32)[0])
    value_position = IntEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Int)


def _serialise_long(builder, data, source):
    LongStart(builder)
    LongAddValue(builder, data.astype(np.int64)[0])
    value_position = LongEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Long)


def _serialise_float(builder, data, source):
    FloatStart(builder)
    FloatAddValue(builder, data.astype(np.float64)[0])
    value_position = FloatEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Float)


def _serialise_double(builder, data, source):
    DoubleStart(builder)
    DoubleAddValue(builder, data.astype(np.float64)[0])
    value_position = DoubleEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Double)


def _serialise_string(builder, data, source):
    StringStart(builder)
    StringAddValue(builder, data.astype(np.unicode_)[0])
    value_position = StringEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.String)


map_scalar_type_to_serialiser = {
    np.byte: _serialise_byte,
    np.ubyte: _serialise_ubyte,
    np.int8: _serialise_short,
    np.int16: _serialise_short,
    np.int32: _serialise_int,
    np.int64: _serialise_long,
    np.uint8: "",
    np.uint16: "",
    np.uint32: "",
    np.uint64: "",
    np.float32: _serialise_float,
    np.float64: _serialise_double,
}


def _ensure_data_is_numpy_type(data: Any) -> np.ndarray:
    if not isinstance(data, np.ndarray):
        return np.array(data)
    return data


def serialise_f142(data: Any, timestamp_unix_ns: int = 0) -> bytearray:
    """
    Serialise data and corresponding timestamp as an f142 Flatbuffer message.
    Should automagically use a sensible type for data in the message, but if
    in doubt pass data in as a numpy ndarray of a carefully chosen dtype.

    :param data: only scalar value currently supported; if ndarray then ndim must be 0
    :param timestamp_unix_ns: timestamp corresponding to data, e.g. when data was measured, in nanoseconds
    """
    builder, source = _setup_builder()

    data = _ensure_data_is_numpy_type(data)

    if data.ndim != 0:
        raise NotImplementedError("serialise_f142 does not yet support array types")

    # We can use a dictionary to map most numpy types to one of the types defined in the flatbuffer schema
    # but we have to handle strings separately as there are many subtypes
    if np.issubdtype(data.dtype, np.unicode_):
        _serialise_string(builder, data, source)
    else:
        try:
            map_scalar_type_to_serialiser[data.dtype](builder, data, source)
        except KeyError:
            # There are a few numpy types we don't try to handle, for example complex numbers
            raise Exception(
                f"Cannot serialise data of type {data.dtype}, must use one of "
                f"{list(map_scalar_type_to_serialiser.keys()).append(np.unicode_)}"
            )

    return _complete_buffer(builder, timestamp_unix_ns)
