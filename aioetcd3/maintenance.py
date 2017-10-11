import functools

from aioetcd3._etcdv3 import rpc_pb2 as rpc
from aioetcd3.base import StubMixin
import aioetcd3._etcdv3.rpc_pb2_grpc as stub


class Status(object):
    def __init__(self, status):
        self.version = status.version
        self.dbSize = status.dbSize
        self.leader = status.leader
        self.raftIndex = status.raftIndex
        self.raftTerm = status.raftTerm


def call_grpc(request, response_func, method):

    def _f(f):
        @functools.wraps(f)
        async def call(self, *args, **kwargs):
            r = await self.grpc_call(method(self), request(*args, **kwargs))
            return response_func(r)

        return call

    return _f


class Maintenance(StubMixin):
    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._maintenance_stub = stub.MaintenanceStub(channel)

    @call_grpc(lambda: rpc.StatusRequest(), lambda r: Status(r),
               lambda s: s._maintenance_stub.Status)
    async def status(self):
        pass

    @call_grpc(lambda action, type, mid: rpc.AlarmRequest(action=action, memberID=mid, alarm=type),
               lambda r: [m for m in r.alarms], lambda s: s._maintenance_stub.Alarm)
    async def alarm(self, action, type, mid=0):
        pass

    @call_grpc(lambda: rpc.SnapshotRequest(), lambda r: (r.remaining_bytes, r.blob),
               lambda s: s._maintenance_stub.Snapshot)
    async def snapshot(self):
        pass

    @call_grpc(lambda: rpc.HashRequest(), lambda r: r.hash,
               lambda s: s._maintenance_stub.Hash)
    async def hash(self):
        pass

    @call_grpc(lambda: rpc.DefragmentRequest(), lambda r: None,
               lambda s: s._maintenance_stub.Defragment)
    async def defragment(self):
        pass
