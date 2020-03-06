from collections import namedtuple
import flatbuffers
from streaming_data_types.fbschemas.nicos_cache_ns10 import CacheEntry
from streaming_data_types.utils import check_schema_identifier


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
    check_schema_identifier(buf, FILE_IDENTIFIER)

    entry = CacheEntry.CacheEntry.GetRootAsCacheEntry(buf, 0)

    key = entry.Key() if entry.Key() else b""
    time_stamp = entry.Time()
    ttl = entry.Ttl() if entry.Ttl() else 0
    expired = entry.Expired() if entry.Expired() else False
    value = entry.Value() if entry.Value() else b""

    Entry = namedtuple("Entry", "key time_stamp ttl expired value")

    return Entry(key.decode(), time_stamp, ttl, expired, value.decode())
