import unittest
import asyncio
import functools

from aioetcd3.client import client
from aioetcd3.help import range_all


def asynctest(f):
    @functools.wraps(f)
    def _f(self):
        asyncio.get_event_loop().run_until_complete(f(self))

    return _f


class ClusterTest(unittest.TestCase):
    def setUp(self):
        endpoints = "127.0.0.1:2379"
        self.client = client(endpoint=endpoints)

    @asynctest
    async def test_member(self):
        members = await self.client.member_list()
        self.assertTrue(members)

        m = members[0]
        # urls = [u for u in m.clientURLs]
        # urls = [u.rpartition("//")[2] for u in urls]

        healthy, unhealthy = await self.client.member_healthy([m.clientURLs])
        self.assertTrue(healthy)
        self.assertFalse(unhealthy)

        healthy, unhealthy = await self.client.member_healthy()
        self.assertTrue(healthy)
        self.assertFalse(unhealthy)

    @asynctest
    async def tearDown(self):
        await self.client.close()
