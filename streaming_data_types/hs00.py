from functools import reduce
import operator
import flatbuffers
import numpy as np
import streaming_data_types.fbschemas.hs00.ArrayDouble as ArrayDouble
import streaming_data_types.fbschemas.hs00.DimensionMetaData as DimensionMetaData
import streaming_data_types.fbschemas.hs00.EventHistogram as EventHistogram
from streaming_data_types.fbschemas.hs00.Array import Array


FILE_IDENTIFIER = b"hs00"


def get_schema(buf):
    """
    Extract the schema code embedded in the buffer

    :param buf: The raw buffer of the FlatBuffers message.
    :return: The schema name
    """
    return buf[4:8].decode("utf-8")


def deserialise_hs00(buf):
    """
    Convert flatbuffer into a histogram.

    :param buf:
    :return: dict of histogram information
    """
    # Check schema is correct
    if get_schema(buf) != "hs00":
        raise RuntimeError(f"Incorrect schema: expected hs00 but got {get_schema(buf)}")

    event_hist = EventHistogram.EventHistogram.GetRootAsEventHistogram(buf, 0)

    dims = []
    for i in range(event_hist.DimMetadataLength()):
        bins_fb = event_hist.DimMetadata(i).BinBoundaries()

        # Get bins
        temp = ArrayDouble.ArrayDouble()
        temp.Init(bins_fb.Bytes, bins_fb.Pos)
        bins = temp.ValueAsNumpy()

        # Get type
        if event_hist.DimMetadata(i).BinBoundariesType() == Array.ArrayDouble:
            bin_type = np.float64
        else:
            raise TypeError("Type of the bin boundaries is incorrect")

        hist_info = {
            "length": event_hist.DimMetadata(i).Length(),
            "edges": bins.tolist(),
            "type": bin_type,
            "unit": event_hist.DimMetadata(i).Unit().decode("utf-8"),
            "label": event_hist.DimMetadata(i).Label().decode("utf-8"),
        }
        dims.append(hist_info)

    metadata_timestamp = event_hist.LastMetadataTimestamp()

    # Get the data
    if event_hist.DataType() != Array.ArrayDouble:
        raise TypeError("Type of the data array is incorrect")

    data_fb = event_hist.Data()
    temp = ArrayDouble.ArrayDouble()
    temp.Init(data_fb.Bytes, data_fb.Pos)
    shape = event_hist.CurrentShapeAsNumpy().tolist()
    data = temp.ValueAsNumpy().reshape(shape)

    # Get the errors
    errors_fb = event_hist.Errors()
    if errors_fb:
        temp = ArrayDouble.ArrayDouble()
        temp.Init(errors_fb.Bytes, errors_fb.Pos)
        errors = temp.ValueAsNumpy().reshape(shape)
    else:
        errors = []


    hist = {
        "source": event_hist.Source().decode("utf-8") if event_hist.Source() else "",
        "timestamp": event_hist.Timestamp(),
        "shape": shape,
        "dims": dims,
        "data": data,
        "errors": errors,
        "last_metadata_timestamp": metadata_timestamp,
        "info": event_hist.Info().decode("utf-8") if event_hist.Info() else "",
    }
    return hist


def _serialise_metadata(builder, length, edges, unit, label):
    unit_encoded = builder.CreateString(unit)
    label_encoded = builder.CreateString(label)

    ArrayDouble.ArrayDoubleStartValueVector(builder, len(edges))
    # FlatBuffers builds arrays backwards
    for x in reversed(edges):
        builder.PrependFloat64(x)
    bins = builder.EndVector(len(edges))
    # Add the bins
    ArrayDouble.ArrayDoubleStart(builder)
    ArrayDouble.ArrayDoubleAddValue(builder, bins)
    pos_bin = ArrayDouble.ArrayDoubleEnd(builder)

    DimensionMetaData.DimensionMetaDataStart(builder)
    DimensionMetaData.DimensionMetaDataAddLength(builder, length)
    DimensionMetaData.DimensionMetaDataAddBinBoundaries(builder, pos_bin)
    DimensionMetaData.DimensionMetaDataAddBinBoundariesType(builder, Array.ArrayDouble)
    DimensionMetaData.DimensionMetaDataAddLabel(builder, label_encoded)
    DimensionMetaData.DimensionMetaDataAddUnit(builder, unit_encoded)
    return DimensionMetaData.DimensionMetaDataEnd(builder)


def serialise_hs00(histogram):
    """
    Serialise a histogram as an hs00 FlatBuffers message.

    :param histogram: A dictionary containing the histogram to serialise.
    """
    source = None
    info = None

    builder = flatbuffers.Builder(1024)
    if "source" in histogram:
        source = builder.CreateString(histogram["source"])
    if "info" in histogram:
        info = builder.CreateString(histogram["info"])

    # Build shape array
    rank = len(histogram["current_shape"])
    EventHistogram.EventHistogramStartCurrentShapeVector(builder, rank)
    # FlatBuffers builds arrays backwards
    for s in reversed(histogram["current_shape"]):
        builder.PrependUint32(s)
    shape = builder.EndVector(rank)

    # Build dimensions metadata
    metadata = []
    for meta in histogram["dim_metadata"]:
        unit = "" if "unit" not in meta else meta["unit"]
        label = "" if "label" not in meta else meta["label"]
        metadata.append(
            _serialise_metadata(
                builder, meta["length"], meta["bin_boundaries"], unit, label
            )
        )

    EventHistogram.EventHistogramStartDimMetadataVector(builder, rank)
    # FlatBuffers builds arrays backwards
    for m in reversed(metadata):
        builder.PrependUOffsetTRelative(m)
    metadata_vector = builder.EndVector(rank)

    # Build the data
    data_len = reduce(operator.mul, histogram["current_shape"], 1)

    ArrayDouble.ArrayDoubleStartValueVector(builder, data_len)
    # FlatBuffers builds arrays backwards
    for x in reversed(histogram["data"]):
        builder.PrependFloat64(x)
    data = builder.EndVector(data_len)
    ArrayDouble.ArrayDoubleStart(builder)
    ArrayDouble.ArrayDoubleAddValue(builder, data)
    pos_data = ArrayDouble.ArrayDoubleEnd(builder)

    if "errors" in histogram:
        ArrayDouble.ArrayDoubleStartValueVector(builder, data_len)
        for x in reversed(histogram["errors"]):
            builder.PrependFloat64(x)
        errors = builder.EndVector(data_len)
        ArrayDouble.ArrayDoubleStart(builder)
        ArrayDouble.ArrayDoubleAddValue(builder, errors)
        pos_errors = ArrayDouble.ArrayDoubleEnd(builder)

    # Build the actual buffer
    EventHistogram.EventHistogramStart(builder)
    if info:
        EventHistogram.EventHistogramAddInfo(builder, info)
    EventHistogram.EventHistogramAddData(builder, pos_data)
    EventHistogram.EventHistogramAddCurrentShape(builder, shape)
    EventHistogram.EventHistogramAddDimMetadata(builder, metadata_vector)
    EventHistogram.EventHistogramAddTimestamp(builder, histogram["timestamp"])
    if source:
        EventHistogram.EventHistogramAddSource(builder, source)
    EventHistogram.EventHistogramAddDataType(builder, Array.ArrayDouble)
    if "errors" in histogram:
        EventHistogram.EventHistogramAddErrors(builder, pos_errors)
        EventHistogram.EventHistogramAddErrorsType(builder, Array.ArrayDouble)
    if "last_metadata_timestamp" in histogram:
        EventHistogram.EventHistogramAddLastMetadataTimestamp(builder, histogram["last_metadata_timestamp"])
    hist = EventHistogram.EventHistogramEnd(builder)
    builder.Finish(hist)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = FILE_IDENTIFIER
    return buff
