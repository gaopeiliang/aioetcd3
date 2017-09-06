from aioetcd3.utils import increment_last_byte, to_bytes
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
    return key, increment_last_byte(to_bytes(key))


def range_greater(key):
    return key, b'\0'


def range_less(key):
    pass


def range_all():
    return b'\0', b'\0'