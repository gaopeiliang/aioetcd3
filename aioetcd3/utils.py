def to_bytes(maybe_bytestring):
    """
    Encode string to bytes.

    Convenience function to do a simple encode('utf-8') if the input is not
    already bytes. Returns the data unmodified if the input is bytes.
    """
    if isinstance(maybe_bytestring, bytes):
        return maybe_bytestring
    else:
        return maybe_bytestring.encode('utf-8')


def increment_last_byte(byte_string):
    s = bytearray(byte_string)
    s[-1] = s[-1] + 1
    return bytes(s)


def put_key_range(obj, key_range):
    if isinstance(key_range, str) or isinstance(key_range, bytes):
        obj.key = to_bytes(key_range)
    else:
        try:
            key, range_end = key_range
        except Exception:
            raise ValueError("key_range must be either a str/bytes 'key', or ('key', 'range_end') tuple")
        obj.key = to_bytes(key)
        obj.range_end = to_bytes(range_end)
    return obj

