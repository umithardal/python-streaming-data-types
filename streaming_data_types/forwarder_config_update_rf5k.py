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


def serialise_rf5k(config_change: UpdateType, streams: List[StreamInfo]) -> bytes:
    """
    Serialise config update message as an rf5k FlatBuffers message.

    :param config_change:
    :param streams: channel, schema and output topic configurations
    :return:
    """

    builder = flatbuffers.Builder(1024)

    if streams:
        streams_offset = ConfigUpdate.ConfigUpdateStartStreamsVector(
            builder, len(streams)
        )
        for stream in streams:
            channel_offset = builder.CreateString(stream.channel)
            schema_offset = builder.CreateString(stream.schema)
            topic_offset = builder.CreateString(stream.topic)
            Stream.StreamStart(builder)
            Stream.StreamAddTopic(builder, topic_offset)
            Stream.StreamAddSchema(builder, schema_offset)
            Stream.StreamAddChannel(builder, channel_offset)
            stream_offset = Stream.StreamEnd(builder)
            builder.PrependUOffsetTRelative(stream_offset)
        ConfigUpdate.ConfigUpdateAddStreams(builder, streams_offset)

    # Build the actual buffer
    ConfigUpdate.ConfigUpdateStart(builder)
    data = ConfigUpdate.ConfigUpdateEnd(builder)
    ConfigUpdate.ConfigUpdateAddConfigChange(builder, config_change)
    builder.Finish(data)

    # Generate the output and replace the file_identifier
    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER
    return bytes(buffer)
