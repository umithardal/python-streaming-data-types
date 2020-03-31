from functools import reduce
import operator
import flatbuffers
import streaming_data_types.fbschemas.histogram_hs00.ArrayDouble as ArrayDouble
import streaming_data_types.fbschemas.histogram_hs00.DimensionMetaData as DimensionMetaData
import streaming_data_types.fbschemas.histogram_hs00.EventHistogram as EventHistogram
from streaming_data_types.fbschemas.histogram_hs00.Array import Array
from streaming_data_types.utils import check_schema_identifier


FILE_IDENTIFIER = b"hs00"


def deserialise_hs00(buffer):
    """
    Deserialise flatbuffer hs10 into a histogram.

    :param buffer:
    :return: dict of histogram information
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)
    event_hist = EventHistogram.EventHistogram.GetRootAsEventHistogram(buffer, 0)

    dims = []
    for i in range(event_hist.DimMetadataLength()):
        bins_offset = event_hist.DimMetadata(i).BinBoundaries()

        # Get bins
        bins_fb = ArrayDouble.ArrayDouble()
        bins_fb.Init(bins_offset.Bytes, bins_offset.Pos)
        bin_boundaries = bins_fb.ValueAsNumpy()

        # Check type
        if event_hist.DimMetadata(i).BinBoundariesType() != Array.ArrayDouble:
            raise TypeError("Type of the bin boundaries is incorrect, should be double")

        hist_info = {
            "length": event_hist.DimMetadata(i).Length(),
            "bin_boundaries": bin_boundaries,
            "unit": event_hist.DimMetadata(i).Unit().decode("utf-8"),
            "label": event_hist.DimMetadata(i).Label().decode("utf-8"),
        }
        dims.append(hist_info)

    metadata_timestamp = event_hist.LastMetadataTimestamp()

    # Get the data
    if event_hist.DataType() != Array.ArrayDouble:
        raise TypeError("Type of the data array is incorrect")

    data_offset = event_hist.Data()
    data_fb = ArrayDouble.ArrayDouble()
    data_fb.Init(data_offset.Bytes, data_offset.Pos)
    shape = event_hist.CurrentShapeAsNumpy().tolist()
    data = data_fb.ValueAsNumpy().reshape(shape)

    # Get the errors
    errors_offset = event_hist.Errors()
    if errors_offset:
        errors_fb = ArrayDouble.ArrayDouble()
        errors_fb.Init(errors_offset.Bytes, errors_offset.Pos)
        errors = errors_fb.ValueAsNumpy().reshape(shape)
    else:
        errors = []

    hist = {
        "source": event_hist.Source().decode("utf-8") if event_hist.Source() else "",
        "timestamp": event_hist.Timestamp(),
        "current_shape": shape,
        "dim_metadata": dims,
        "data": data,
        "errors": errors,
        "last_metadata_timestamp": metadata_timestamp,
        "info": event_hist.Info().decode("utf-8") if event_hist.Info() else "",
    }
    return hist


def _serialise_metadata(builder, length, edges, unit, label):
    unit_offset = builder.CreateString(unit)
    label_offset = builder.CreateString(label)

    ArrayDouble.ArrayDoubleStartValueVector(builder, len(edges))
    # FlatBuffers builds arrays backwards
    for x in reversed(edges):
        builder.PrependFloat64(x)
    bins_vector = builder.EndVector(len(edges))
    # Add the bins
    ArrayDouble.ArrayDoubleStart(builder)
    ArrayDouble.ArrayDoubleAddValue(builder, bins_vector)
    bins_offset = ArrayDouble.ArrayDoubleEnd(builder)

    DimensionMetaData.DimensionMetaDataStart(builder)
    DimensionMetaData.DimensionMetaDataAddLength(builder, length)
    DimensionMetaData.DimensionMetaDataAddBinBoundaries(builder, bins_offset)
    DimensionMetaData.DimensionMetaDataAddBinBoundariesType(builder, Array.ArrayDouble)
    DimensionMetaData.DimensionMetaDataAddLabel(builder, label_offset)
    DimensionMetaData.DimensionMetaDataAddUnit(builder, unit_offset)
    return DimensionMetaData.DimensionMetaDataEnd(builder)


def serialise_hs00(histogram):
    """
    Serialise a histogram as an hs00 FlatBuffers message.

    :param histogram: A dictionary containing the histogram to serialise.
    """
    source_offset = None
    info_offset = None

    builder = flatbuffers.Builder(1024)
    if "source" in histogram:
        source_offset = builder.CreateString(histogram["source"])
    if "info" in histogram:
        info_offset = builder.CreateString(histogram["info"])

    # Build shape array
    rank = len(histogram["current_shape"])
    EventHistogram.EventHistogramStartCurrentShapeVector(builder, rank)
    # FlatBuffers builds arrays backwards
    for s in reversed(histogram["current_shape"]):
        builder.PrependUint32(s)
    shape_offset = builder.EndVector(rank)

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
    for x in reversed(histogram["data"].flatten()):
        builder.PrependFloat64(x)
    data_vector = builder.EndVector(data_len)
    ArrayDouble.ArrayDoubleStart(builder)
    ArrayDouble.ArrayDoubleAddValue(builder, data_vector)
    data_offset = ArrayDouble.ArrayDoubleEnd(builder)

    errors_offset = None
    if "errors" in histogram:
        ArrayDouble.ArrayDoubleStartValueVector(builder, data_len)
        for x in reversed(histogram["errors"].flatten()):
            builder.PrependFloat64(x)
        errors = builder.EndVector(data_len)
        ArrayDouble.ArrayDoubleStart(builder)
        ArrayDouble.ArrayDoubleAddValue(builder, errors)
        errors_offset = ArrayDouble.ArrayDoubleEnd(builder)

    # Build the actual buffer
    EventHistogram.EventHistogramStart(builder)
    if info_offset:
        EventHistogram.EventHistogramAddInfo(builder, info_offset)
    EventHistogram.EventHistogramAddData(builder, data_offset)
    EventHistogram.EventHistogramAddCurrentShape(builder, shape_offset)
    EventHistogram.EventHistogramAddDimMetadata(builder, metadata_vector)
    EventHistogram.EventHistogramAddTimestamp(builder, histogram["timestamp"])
    if source_offset:
        EventHistogram.EventHistogramAddSource(builder, source_offset)
    EventHistogram.EventHistogramAddDataType(builder, Array.ArrayDouble)
    if errors_offset:
        EventHistogram.EventHistogramAddErrors(builder, errors_offset)
        EventHistogram.EventHistogramAddErrorsType(builder, Array.ArrayDouble)
    if "last_metadata_timestamp" in histogram:
        EventHistogram.EventHistogramAddLastMetadataTimestamp(
            builder, histogram["last_metadata_timestamp"]
        )
    hist_message = EventHistogram.EventHistogramEnd(builder)
    builder.Finish(hist_message)

    # Generate the output and replace the file_identifier
    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER
    return bytes(buffer)
