from aioetcd3.rpc import rpc_pb2 as rpc
from aioetcd3.base import StubMixin


class RLease(object):
    def __init__(self, ttl, id, client):
        self.ttl = ttl
        self.id = id
        self.client = client

    async def revoke(self):
        return await self.client.revoke_lease(self.id)

    async def refresh(self):
        return await self.client.refresh_lease(self.id)

    async def info(self):
        return await self.client.get_lease_info(self.id)


class Lease(StubMixin):

    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._lease_stub = rpc.LeaseStub(channel)

    async def grant_lease(self, ttl, id=0):
        lease_request = rpc.LeaseGrantRequest(TTL=ttl, ID=id)

        lease_reponse = await self._lease_stub.LeaseGrant(lease_request, self.timeout)

        return RLease(lease_reponse.TTL, lease_reponse.ID, self)

    async def revoke_lease(self, lease):
        lease_id = get_lease_id(lease)
        lease_request = rpc.LeaseRevokeRequest(ID=lease_id)

        await self._lease_stub.LeaseRevoke(lease_request, self.timeout)

    async def refresh_lease(self, lease):
        lease_id = get_lease_id(lease)
        lease_request = rpc.LeaseKeepAliveRequest(ID=lease_id)

        async def generate_request():
            for request in [lease_request]:
                yield request

        new_lease = None
        async with self._lease_stub.LeaseKeepAlive.with_scope(generate_request()) as result:
            async for r in result:
                new_lease = RLease(r.TTL, r.ID, self)

        return new_lease

    async def get_lease_info(self, lease):

        lease_id = get_lease_id(lease)

        request = rpc.LeaseTimeToLiveRequest(ID=lease_id, keys=True)

        response = await self._lease_stub.LeaseTimeToLive(request)

        if response.TTL >= 0:
            return RLease(response.TTL, response.ID, self), [k for k in response.keys]
        else:
            return None, []


def get_lease_id(lease):
    if hasattr(lease, 'id'):
        return lease.id
    else:
        return lease

