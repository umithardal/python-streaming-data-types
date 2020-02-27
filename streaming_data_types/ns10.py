import flatbuffers
from streaming_data_types.fbschemas.ns10 import CacheEntry
from streaming_data_types.utils import get_schema


FILE_IDENTIFIER = b"ns10"


def serialise_ns10(cache_entry):
    builder = flatbuffers.Builder(128)

    value = builder.CreateString(cache_entry["value"])
    key = builder.CreateString(cache_entry["key"])

    ttl = cache_entry["ttl"] if "ttl" in cache_entry else False
    time_stamp = cache_entry["time"] if "time" in cache_entry else 0
    expired = cache_entry["expired"] if "expired" in cache_entry else False

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
    buff[4:8] = b"ns10"

    return buff


def deserialise_ns10(buf):
    # Check schema is correct
    if get_schema(buf) != FILE_IDENTIFIER.decode():
        raise RuntimeError(
            f"Incorrect schema: expected {FILE_IDENTIFIER} but got {get_schema(buf)}"
        )

    entry = CacheEntry.CacheEntry.GetRootAsCacheEntry(buf, 0)

    key = entry.Key() if entry.Key() else ""
    time_stamp = entry.Time()
    ttl = entry.Ttl() if entry.Ttl() else 0
    expired = entry.Expired() if entry.Expired() else False
    value = entry.Value() if entry.Value() else ""

    cache_entry = {
        "key": key.decode("utf-8"),
        "time": time_stamp,
        "ttl": ttl,
        "expired": expired,
        "value": value.decode("utf-8"),
    }

    return cache_entry
