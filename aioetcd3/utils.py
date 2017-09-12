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
    s = bytearray(to_bytes(byte_string))
    for i in range(len(s) - 1, -1, -1):
        if s[i] < 0xff:
            s[i] += 1
            return bytes(s[:i+1])
    else:
        return b'\x00'


def next_valid_key(byte_string):
    return to_bytes(byte_string) + b'\x00'


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


def ipv4_endpoints(server_list):
    return 'ipv4:///' + ','.join(server_list)


def ipv6_endpoints(server_list):
    return 'ipv6:///' + ','.join(server_list)


def dns_endpoint(dns_name):
    return 'dns:///' + dns_name


def get_secure_creds(ca_cert, cert_key, cert_cert):
    with open(ca_cert, 'rb') as ca_cert_file:
        with open(cert_key, 'rb') as cert_key_file:
            with open(cert_cert, 'rb') as cert_cert_file:
                return ca_cert_file.read(), cert_key_file.read(), cert_cert_file.read()

