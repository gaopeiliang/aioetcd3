from aioetcd3.rpc import rpc_pb2 as rpc


class RLease(object):
    def __init__(self, ttl, id, client):
        self.ttl = ttl
        self.id = id
        self.client = client


class Lease(object):
    def __init__(self, client, channel, timeout):
        self.client = client
        self.stub = rpc.LeaseStub(channel=channel)
        self.timeout = timeout

    async def lease(self, ttl, id=0):
        lease_request = rpc.LeaseGrantRequest(TTL=ttl, ID=id)

        lease_reponse = await self.stub.LeaseGrant(lease_request, self.timeout)

        return RLease(lease_reponse.TTL, lease_reponse.ID, self)

    async def revoke_lease(self, id):
        lease_request = rpc.LeaseRevokeRequest(ID=id)

        await self.stub.LeaseRevoke(lease_request, self.timeout)

    async def refresh_lease(self, id):
        lease_request = rpc.LeaseKeepAliveRequest(ID=id)

        async def generate_request():
            for request in [lease_request]:
                yield request

        async with self.stub.LeaseKeepAlive.with_scope(generate_request()) as result:
            async for r in result:
                yield RLease(r.TTL, r.ID, self)

