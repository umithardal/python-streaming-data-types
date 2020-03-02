from collections import namedtuple
import flatbuffers
from streaming_data_types.fbschemas.nicos_cache_ns10 import CacheEntry
from streaming_data_types.utils import get_schema


FILE_IDENTIFIER = b"ns10"


def serialise_ns10(
    key: str, value: str, time_stamp: float = 0, ttl: float = 0, expired: bool = False
):
    builder = flatbuffers.Builder(128)

    value = builder.CreateString(value)
    key = builder.CreateString(key)

    CacheEntry.CacheEntryStart(builder)
    CacheEntry.CacheEntryAddValue(builder, value)
    CacheEntry.CacheEntryAddExpired(builder, expired)
    CacheEntry.CacheEntryAddTtl(builder, ttl)
    CacheEntry.CacheEntryAddTime(builder, time_stamp)
    CacheEntry.CacheEntryAddKey(builder, key)
    entry = CacheEntry.CacheEntryEnd(builder)
    builder.Finish(entry)

    # Generate the output and replace the file_identifier
    buff = builder.Output()
    buff[4:8] = FILE_IDENTIFIER

    return buff


def deserialise_ns10(buf):
    # Check schema is correct
    if get_schema(buf) != FILE_IDENTIFIER.decode():
        raise RuntimeError(
            f"Incorrect schema: expected {FILE_IDENTIFIER} but got {get_schema(buf)}"
        )

    entry = CacheEntry.CacheEntry.GetRootAsCacheEntry(buf, 0)

    key = entry.Key() if entry.Key() else b""
    time_stamp = entry.Time()
    ttl = entry.Ttl() if entry.Ttl() else 0
    expired = entry.Expired() if entry.Expired() else False
    value = entry.Value() if entry.Value() else b""

    Entry = namedtuple("Entry", "key time_stamp ttl expired value")

    return Entry(key.decode(), time_stamp, ttl, expired, value.decode())
