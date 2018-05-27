import functools
import aiogrpc
import grpc

from aioetcd3._etcdv3 import rpc_pb2 as rpc
from aioetcd3.base import StubMixin
import aioetcd3._etcdv3.rpc_pb2_grpc as stub
from aioetcd3.utils import ipv4_endpoints
from aioetcd3.maintenance import Maintenance


def call_grpc(request, response_func, method):

    def _f(f):
        @functools.wraps(f)
        async def call(self, *args, **kwargs):
            r = await self.grpc_call(method(self), request(*args, **kwargs))
            return response_func(r)

        return call

    return _f


class Cluster(StubMixin):
    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._cluster_stub = stub.ClusterStub(channel)

    @call_grpc(lambda peerurls: rpc.MemberAddRequest(peerURLs=peerurls),
               lambda r: r.member, lambda s: s._cluster_stub.MemberAdd)
    async def member_add(self, peerurls):
        pass

    @call_grpc(lambda mid: rpc.MemberRemoveRequest(ID=mid),
               lambda r: [m for m in r.members],
               lambda s: s._cluster_stub.MemberRemove)
    async def member_remove(self, mid):
        pass

    @call_grpc(lambda mid, urls: rpc.MemberUpdateRequest(ID=mid, peerURLs=urls),
               lambda r: [m for m in r.members],
               lambda s: s._cluster_stub.MemberUpdate)
    async def member_update(self, mid, peerurls):
        pass

    @call_grpc(lambda: rpc.MemberListRequest(), lambda r: [m for m in r.members],
               lambda s: s._cluster_stub.MemberList)
    async def member_list(self):
        pass

    async def member_healthy(self, members=None):

        if not members:
            members = await self.member_list()
            members = [m.clientURLs for m in members]

        health_members = []
        unhealth_members = []
        for m in members:

            m = [u.rpartition("//")[2] for u in m]
            m = [u for u in m if u]
            if m:
                server_endpoint = ipv4_endpoints(m)
    
                if self._credentials:
                    channel = aiogrpc.secure_channel(server_endpoint, self._credentials, options=self._options,
                                                     loop=self._loop, executor=self._executor,
                                                     standalone_pool_for_streaming=True)
                else:
                    channel = aiogrpc.insecure_channel(server_endpoint, options=self._options, loop=self._loop,
                                                       executor=self._executor, standalone_pool_for_streaming=True)
                try:
                    maintenance = Maintenance(channel=channel, timeout=2)
                    try:
                        await maintenance.status()
                    except grpc.RpcError:
                        unhealth_members.append(m)
                    else:
                        health_members.append(m)
                finally:
                    await channel.close()
            else:
                unhealth_members.append(m)

        return health_members, unhealth_members


