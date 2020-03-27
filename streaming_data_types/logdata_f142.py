import flatbuffers
from streaming_data_types.fbschemas.logdata_f142 import LogData
from streaming_data_types.fbschemas.logdata_f142.Value import Value
from streaming_data_types.fbschemas.logdata_f142.UByte import (
    UByte,
    UByteStart,
    UByteAddValue,
    UByteEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Byte import (
    Byte,
    ByteStart,
    ByteAddValue,
    ByteEnd,
)
from streaming_data_types.fbschemas.logdata_f142.UShort import (
    UShort,
    UShortStart,
    UShortAddValue,
    UShortEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Short import (
    Short,
    ShortStart,
    ShortAddValue,
    ShortEnd,
)
from streaming_data_types.fbschemas.logdata_f142.UInt import (
    UInt,
    UIntStart,
    UIntAddValue,
    UIntEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Int import (
    Int,
    IntStart,
    IntAddValue,
    IntEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ULong import (
    ULong,
    ULongStart,
    ULongAddValue,
    ULongEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Long import (
    Long,
    LongStart,
    LongAddValue,
    LongEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Float import (
    Float,
    FloatStart,
    FloatAddValue,
    FloatEnd,
)
from streaming_data_types.fbschemas.logdata_f142.Double import (
    Double,
    DoubleStart,
    DoubleAddValue,
    DoubleEnd,
)
from streaming_data_types.fbschemas.logdata_f142.String import (
    String,
    StringStart,
    StringAddValue,
    StringEnd,
)
from streaming_data_types.fbschemas.logdata_f142.ArrayByte import ArrayByte
from streaming_data_types.fbschemas.logdata_f142.ArrayUByte import ArrayUByte
from streaming_data_types.fbschemas.logdata_f142.ArrayShort import ArrayShort
from streaming_data_types.fbschemas.logdata_f142.ArrayUShort import ArrayUShort
from streaming_data_types.fbschemas.logdata_f142.ArrayInt import ArrayInt
from streaming_data_types.fbschemas.logdata_f142.ArrayUInt import ArrayUInt
from streaming_data_types.fbschemas.logdata_f142.ArrayLong import ArrayLong
from streaming_data_types.fbschemas.logdata_f142.ArrayULong import ArrayULong
from streaming_data_types.fbschemas.logdata_f142.ArrayFloat import ArrayFloat
from streaming_data_types.fbschemas.logdata_f142.ArrayDouble import ArrayDouble
from streaming_data_types.fbschemas.logdata_f142.ArrayString import ArrayString
from streaming_data_types.utils import check_schema_identifier
import numpy as np
from typing import Any, Tuple, NamedTuple
from collections import namedtuple


FILE_IDENTIFIER = b"f142"


def _complete_buffer(builder, timestamp_unix_ns: int) -> bytearray:
    LogData.LogDataAddTimestamp(builder, timestamp_unix_ns)
    log_msg = LogData.LogDataEnd(builder)
    builder.Finish(log_msg)
    buff = builder.Output()
    buff[4:8] = FILE_IDENTIFIER
    return buff


def _setup_builder(source_name: str) -> Tuple[flatbuffers.Builder, int]:
    builder = flatbuffers.Builder(1024)
    source = builder.CreateString(source_name)
    return builder, source


def _serialise_byte(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ByteStart(builder)
    ByteAddValue(builder, data.item())
    value_position = ByteEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Byte)


def _serialise_ubyte(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    UByteStart(builder)
    UByteAddValue(builder, data.item())
    value_position = UByteEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.UByte)


def _serialise_short(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ShortStart(builder)
    ShortAddValue(builder, data.item())
    value_position = ShortEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Short)


def _serialise_ushort(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    UShortStart(builder)
    UShortAddValue(builder, data.item())
    value_position = UShortEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.UShort)


def _serialise_int(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    IntStart(builder)
    IntAddValue(builder, data.item())
    value_position = IntEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Int)


def _serialise_uint(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    UIntStart(builder)
    UIntAddValue(builder, data.item())
    value_position = UIntEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.UInt)


def _serialise_long(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    LongStart(builder)
    LongAddValue(builder, data.item())
    value_position = LongEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Long)


def _serialise_ulong(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    ULongStart(builder)
    ULongAddValue(builder, data.item())
    value_position = ULongEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.ULong)


def _serialise_float(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    FloatStart(builder)
    FloatAddValue(builder, data.item())
    value_position = FloatEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Float)


def _serialise_double(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    DoubleStart(builder)
    DoubleAddValue(builder, data.item())
    value_position = DoubleEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.Double)


def _serialise_string(builder: flatbuffers.Builder, data: np.ndarray, source: int):
    StringStart(builder)
    StringAddValue(builder, data.item())
    value_position = StringEnd(builder)
    LogData.LogDataStart(builder)
    LogData.LogDataAddSourceName(builder, source)
    LogData.LogDataAddValue(builder, value_position)
    LogData.LogDataAddValueType(builder, Value.String)


_map_scalar_type_to_serialiser = {
    np.dtype("byte"): _serialise_byte,
    np.dtype("ubyte"): _serialise_ubyte,
    np.dtype("int8"): _serialise_short,
    np.dtype("int16"): _serialise_short,
    np.dtype("int32"): _serialise_int,
    np.dtype("int64"): _serialise_long,
    np.dtype("uint8"): _serialise_ushort,
    np.dtype("uint16"): _serialise_ushort,
    np.dtype("uint32"): _serialise_uint,
    np.dtype("uint64"): _serialise_ulong,
    np.dtype("float32"): _serialise_float,
    np.dtype("float64"): _serialise_double,
}


def serialise_f142(
    value: Any, source_name: str, timestamp_unix_ns: int = 0
) -> bytearray:
    """
    Serialise value and corresponding timestamp as an f142 Flatbuffer message.
    Should automagically use a sensible type for value in the message, but if
    in doubt pass value in as a numpy ndarray of a carefully chosen dtype.

    :param value: only scalar value currently supported; if ndarray then ndim must be 0
    :param source_name: name of the data source
    :param timestamp_unix_ns: timestamp corresponding to value, e.g. when value was measured, in nanoseconds
    """
    builder, source = _setup_builder(source_name)

    value = np.array(value)

    if value.ndim != 0:
        raise NotImplementedError("serialise_f142 does not yet support array types")

    # We can use a dictionary to map most numpy types to one of the types defined in the flatbuffer schema
    # but we have to handle strings separately as there are many subtypes
    if np.issubdtype(value.dtype, np.unicode_):
        _serialise_string(builder, value, source)
    else:
        try:
            _map_scalar_type_to_serialiser[value.dtype](builder, value, source)
        except KeyError:
            # There are a few numpy types we don't try to handle, for example complex numbers
            raise Exception(
                f"Cannot serialise data of type {value.dtype}, must use one of "
                f"{list(_map_scalar_type_to_serialiser.keys()).append(np.unicode_)}"
            )

    return _complete_buffer(builder, timestamp_unix_ns)


map_fb_enum_to_type = {
    Value.Byte: Byte,
    Value.UByte: UByte,
    Value.Short: Short,
    Value.UShort: UShort,
    Value.Int: Int,
    Value.UInt: UInt,
    Value.Long: Long,
    Value.ULong: ULong,
    Value.Float: Float,
    Value.Double: Double,
    Value.String: String,
    Value.ArrayByte: ArrayByte,
    Value.ArrayUByte: ArrayUByte,
    Value.ArrayShort: ArrayShort,
    Value.ArrayUShort: ArrayUShort,
    Value.ArrayInt: ArrayInt,
    Value.ArrayUInt: ArrayUInt,
    Value.ArrayLong: ArrayLong,
    Value.ArrayULong: ArrayULong,
    Value.ArrayFloat: ArrayFloat,
    Value.ArrayDouble: ArrayDouble,
    Value.ArrayString: ArrayString,
}


def deserialise_f142(buffer: bytearray) -> NamedTuple:
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    log_data = LogData.LogData.GetRootAsLogData(buffer, 0)
    source_name = log_data.SourceName() if log_data.SourceName() else b""

    value_offset = log_data.Value()
    value_fb = map_fb_enum_to_type[log_data.ValueType()]()
    value_fb.Init(value_offset.Bytes, value_offset.Pos)
    try:
        value = value_fb.ValueAsNumpy()
    except AttributeError:
        value = np.array(value_fb.Value())

    timestamp = log_data.Timestamp()

    LogDataInfo = namedtuple("LogDataInfo", "value source_name timestamp_unix_ns")
    return LogDataInfo(value, source_name.decode(), timestamp)
