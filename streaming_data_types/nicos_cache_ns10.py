from collections import namedtuple
import flatbuffers
from streaming_data_types.fbschemas.nicos_cache_ns10 import CacheEntry
from streaming_data_types.utils import check_schema_identifier


FILE_IDENTIFIER = b"ns10"


def serialise_ns10(key, value, time_stamp=0, ttl=0, expired=False):
    builder = flatbuffers.Builder(128)

    value_offset = builder.CreateString(value)
    key_offset = builder.CreateString(key)

    CacheEntry.CacheEntryStart(builder)
    CacheEntry.CacheEntryAddValue(builder, value_offset)
    CacheEntry.CacheEntryAddExpired(builder, expired)
    CacheEntry.CacheEntryAddTtl(builder, ttl)
    CacheEntry.CacheEntryAddTime(builder, time_stamp)
    CacheEntry.CacheEntryAddKey(builder, key_offset)
    cache_entry_message = CacheEntry.CacheEntryEnd(builder)
    builder.Finish(cache_entry_message)

    # Generate the output and replace the file_identifier
    buffer = builder.Output()
    buffer[4:8] = FILE_IDENTIFIER

    return bytes(buffer)


def deserialise_ns10(buffer):
    check_schema_identifier(buffer, FILE_IDENTIFIER)

    entry = CacheEntry.CacheEntry.GetRootAsCacheEntry(buffer, 0)

    key = entry.Key() if entry.Key() else b""
    time_stamp = entry.Time()
    ttl = entry.Ttl() if entry.Ttl() else 0
    expired = entry.Expired() if entry.Expired() else False
    value = entry.Value() if entry.Value() else b""

    Entry = namedtuple("Entry", "key time_stamp ttl expired value")

    return Entry(key.decode().strip(), time_stamp, ttl, expired, value.decode())
