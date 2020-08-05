from aioetcd3._etcdv3 import rpc_pb2 as rpc
from aioetcd3.base import StubMixin

import functools
import inspect
import asyncio
import aioetcd3._etcdv3.rpc_pb2_grpc as stub


def call_grpc(request, response_func, method):

    def _f(f):
        @functools.wraps(f)
        async def call(self, *args, **kwargs):
            params = inspect.getcallargs(f, self, *args, **kwargs)
            params.pop('self')
            r = await self.grpc_call(method(self), request(**params))
            return response_func(r, client=self)

        return call

    return _f


class RLease(object):
    def __init__(self, ttl, id, client):
        self.ttl = ttl
        self.id = id
        self.client = client

    async def __aenter__(self):
        lease = await self.client.grant_lease(ttl=self.ttl)
        self.ttl = lease.ttl
        self.id = lease.id

        refresh_ttl = self.ttl // 2

        async def task(cycle):
            while True:
                await asyncio.sleep(cycle)
                await self.refresh()

        self.refresh_task = asyncio.ensure_future(task(refresh_ttl))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, 'refresh_task'):
            self.refresh_task.cancel()
            await asyncio.wait([self.refresh_task])
        await self.revoke()

    async def revoke(self):
        return await self.client.revoke_lease(self.id)

    async def refresh(self):
        return await self.client.refresh_lease(self.id)

    async def info(self):
        return await self.client.get_lease_info(self.id)


class Lease(StubMixin):

    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._lease_stub = stub.LeaseStub(channel)

    @call_grpc(lambda ttl, id: rpc.LeaseGrantRequest(TTL=ttl, ID=id),
               lambda r, client: RLease(r.TTL, r.ID, client),
               lambda s: s._lease_stub.LeaseGrant)
    async def grant_lease(self, ttl, id=0):
        pass

    def grant_lease_scope(self, ttl, id=0):
        return RLease(ttl, id, self)

    @call_grpc(lambda lease: rpc.LeaseRevokeRequest(ID=get_lease_id(lease)),
               lambda r, client: None, lambda s: s._lease_stub.LeaseRevoke)
    async def revoke_lease(self, lease):
        pass

    async def refresh_lease(self, lease):
        lease_id = get_lease_id(lease)
        lease_request = rpc.LeaseKeepAliveRequest(ID=lease_id)

        async def generate_request(request):
            for re in [request]:
                yield re

        new_lease = None
        async with self._lease_stub.LeaseKeepAlive.with_scope(generate_request(lease_request)) as result:
            async for r in result:
                self._update_cluster_info(r.header)
                new_lease = RLease(r.TTL, r.ID, self)

        return new_lease

    @call_grpc(lambda lease: rpc.LeaseTimeToLiveRequest(ID=get_lease_id(lease), keys=True),
               lambda r, client: (RLease(r.TTL, r.ID, client), [k for k in r.keys]) if r.TTL >= 0 else (None, []),
               lambda s: s._lease_stub.LeaseTimeToLive)
    async def get_lease_info(self, lease):
        pass


def get_lease_id(lease):
    if hasattr(lease, 'id'):
        return lease.id
    else:
        return lease

