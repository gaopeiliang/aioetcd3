from aioetcd3.utils import increment_last_byte, to_bytes
from aioetcd3._etcdv3 import auth_pb2 as auth
from aioetcd3._etcdv3 import rpc_pb2 as rpc

SORT_ASCEND = 'ascend'
SORT_DESCEND = 'descend'

PER_R = auth.Permission.READ
PER_W = auth.Permission.WRITE
PER_RW = auth.Permission.READWRITE

ALARM_ACTION_GET = rpc.AlarmRequest.GET
ALARM_ACTION_ACTIVATE = rpc.AlarmRequest.ACTIVATE
ALARM_ACTION_DEACTIVATE = rpc.AlarmRequest.DEACTIVATE

ALARM_TYPE_NONE = rpc.NONE
ALARM_TYPE_NOSPACE = rpc.NOSPACE


def range_prefix(key):
    return key, increment_last_byte(to_bytes(key))


def range_greater(key):
    return key, b'\0'


def range_less(key):
    pass


def range_all():
    return b'\0', b'\0'