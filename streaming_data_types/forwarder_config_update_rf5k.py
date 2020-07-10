from collections import namedtuple
import flatbuffers
from streaming_data_types.utils import check_schema_identifier
from streaming_data_types.fbschemas.forwarder_config_update_rf5k import (
    UpdateType,
    ConfigUpdate,
    Stream,
)
from typing import List

FILE_IDENTIFIER = b"rf5k"

ConfigurationUpdate = namedtuple("ConfigurationUpdate", ("config_change", "streams"),)

StreamInfo = namedtuple("StreamInfo", ("channel", "schema", "topic"),)


def deserialise_rf5k(buffer):
    """
    Deserialise FlatBuffer rf5k.

    :param buffer: The FlatBuffers buffer.
    :return: The deserialised data.
    """
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    config_message = ConfigUpdate.ConfigUpdate.GetRootAsConfigUpdate(buffer, 0)

    return ConfigurationUpdate(config_message.ConfigChange(), [])


def serialise_stream(builder, topic_offset, schema_offset, channel_offset):
    Stream.StreamStart(builder)
    Stream.StreamAddTopic(builder, topic_offset)
    Stream.StreamAddSchema(builder, schema_offset)
    Stream.StreamAddChannel(builder, channel_offset)
    return Stream.StreamEnd(builder)


def serialise_rf5k(config_change: UpdateType, streams: List[StreamInfo]) -> bytes:
    """
    Serialise config update message as an rf5k FlatBuffers message.

    :param config_change:
    :param streams: channel, schema and output topic configurations
    :return:
    """
    builder = flatbuffers.Builder(1024)

    if streams:
        # We have to use multiple loops/list comprehensions here because we cannot create strings after we have
        # called StreamStart and cannot create streams after we have called StartVector
        stream_field_offsets = [
            (
                builder.CreateString(stream.channel),
                builder.CreateString(stream.schema),
                builder.CreateString(stream.topic),
            )
            for stream in streams
        ]
        stream_offsets = [
            serialise_stream(builder, *stream_fields)
            for stream_fields in stream_field_offsets
        ]

        ConfigUpdate.ConfigUpdateStartStreamsVector(builder, len(streams))
        for stream_offset in stream_offsets:
            builder.PrependUOffsetTRelative(stream_offset)
        streams_offset = builder.EndVector(len(streams))

    # Build the actual buffer
    ConfigUpdate.ConfigUpdateStart(builder)
    if streams:
        ConfigUpdate.ConfigUpdateAddStreams(builder, streams_offset)
    ConfigUpdate.ConfigUpdateAddConfigChange(builder, config_change)
    data = ConfigUpdate.ConfigUpdateEnd(builder)
    builder.Finish(data)

    # Generate the output and replace the file_identifier
    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER
    return bytes(buffer)
