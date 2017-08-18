import unittest
import asyncio
import functools
from aioetcd3.client import client
from aioetcd3.help import range_all


def asynctest(f):
    @functools.wraps(f)
    def _f(self):
        return asyncio.get_event_loop().run_until_complete(f(self))

    return _f


class KVTest(unittest.TestCase):

    def setUp(self):
        endpoints = "100.73.46.3:2379"
        self.client = client(endpoints=endpoints)

        self.tearDown()

    @asynctest
    async def tearDown(self):
        await self.client.delete(key_range=range_all())

    @asynctest
    async def test_put_get(self):

        for i in range(0, 10):
            key = '/test' + str(i)
            value, meta = await self.client.put(key, i)
            self.assertIsNone(value)
            self.assertIsNone(meta)

        value, meta = await self.client.put('/test9', 10, prev_kv=True)
        self.assertEqual(value, 9)
        self.assertIsNotNone(meta)

        value, meta = await self.client.put('/test9', 9, prev_kv=True, ignore_value=True)
        self.assertEqual(value, 10)
        self.assertIsNotNone(meta)

        value, meta = await self.client.put('/test9', 9, prev_kv=True)
        self.assertEqual(value, 10)
        self.assertIsNotNone(meta)

        count = await self.client.count(key_range=range_all())
        self.assertEqual(count, 10)


if __name__ == '__main__':
    unittest.main()

