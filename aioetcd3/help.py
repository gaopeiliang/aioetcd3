from aioetcd3.utils import increment_last_byte, to_bytes, next_valid_key
from aioetcd3._etcdv3 import auth_pb2 as _auth
from aioetcd3._etcdv3 import rpc_pb2 as _rpc

SORT_ASCEND = 'ascend'
SORT_DESCEND = 'descend'

PER_R = _auth.Permission.READ
PER_W = _auth.Permission.WRITE
PER_RW = _auth.Permission.READWRITE

ALARM_ACTION_GET = _rpc.AlarmRequest.GET
ALARM_ACTION_ACTIVATE = _rpc.AlarmRequest.ACTIVATE
ALARM_ACTION_DEACTIVATE = _rpc.AlarmRequest.DEACTIVATE

ALARM_TYPE_NONE = _rpc.NONE
ALARM_TYPE_NOSPACE = _rpc.NOSPACE


def range_prefix(key):
    if not key:
        return range_all()
    else:
        return to_bytes(key), increment_last_byte(to_bytes(key))


def range_prefix_excluding(prefix, with_out):
    """
    Return a list of key_range, union of which is a prefix range excluding some keys

    :param prefix: the key to generate the range prefix
    :param with_out: a list of key_range (key or (start,end) tuple)
    :return: a list of key_range, union of which is a prefix range excluding some keys
    """
    return range_excluding(range_prefix(prefix), with_out)


def range_excluding(range_, with_out):
    """
    Return a list of key_range, union of which is a range excluding some keys

    :param range_: the original range
    :param with_out: a list of key_range (key or (start,end) tuple)
    :return: a list of key_range, union of which is a prefix range excluding some keys
    """
    # Merge with_out
    with_out_ranges = [(to_bytes(v), next_valid_key(v)) if isinstance(v, str) or isinstance(v, bytes)
                       else (to_bytes(v[0]), to_bytes(v[1]))
                       for v in with_out]
    with_out_ranges.sort()
    range_start, range_end = range_
    range_start = to_bytes(range_start)
    range_end = to_bytes(range_end)
    re_range = []
    next_start_key = range_start
    for s, e in with_out_ranges:
        if s >= range_end != b'\x00':
            break
        start, end = next_start_key, s
        if start < end:
            re_range.append((start, end))
        if e == b'\x00':
            next_start_key = None
            break
        else:
            next_start_key = max(next_start_key, e)
    if next_start_key is not None and \
            (next_start_key < range_end or
                range_end == b'\x00'):
        re_range.append((next_start_key, range_end))
    return re_range


def range_greater(key):
    return next_valid_key(key), b'\0'


def range_greater_equal(key):
    return key, b'\0'


def range_less(key):
    return b'\0', key


def range_less_equal(key):
    return b'\0', next_valid_key(key)


def range_all():
    return b'\0', b'\0'
