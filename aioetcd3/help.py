from aioetcd3.utils import increment_last_byte, to_bytes
from aioetcd3.rpc import auth_pb2 as auth

SORT_ASCEND = 'ascend'
SORT_DESCEND = 'descend'

PER_R = auth.Permission.READ
PER_W = auth.Permission.WRITE
PER_RW = auth.Permission.READWRITE


def range_prefix(key):
    return key, increment_last_byte(to_bytes(key))


def range_greater(key):
    return key, b'\0'


def range_less(key):
    pass


def range_all():
    return b'\0', b'\0'