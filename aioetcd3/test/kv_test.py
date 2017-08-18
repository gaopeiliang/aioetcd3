import unittest
import asyncio
import functools
from aioetcd3.client import client
from aioetcd3.kv import KV
from aioetcd3.help import range_all, range_prefix, range_greater
from aioetcd3 import transaction


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

        value, meta = await self.client.put('/test9', "10", prev_kv=True)
        self.assertEqual(value, b'9')
        self.assertIsNotNone(meta)

        value, meta = await self.client.put('/test9', "9", prev_kv=True, ignore_value=True)
        self.assertEqual(value, b'10')
        self.assertIsNotNone(meta)

        value, meta = await self.client.put('/test9', "9", prev_kv=True)
        self.assertEqual(value, b'10')
        self.assertIsNotNone(meta)

        count = await self.client.count(key_range=range_all())
        self.assertEqual(count, 10)

        value, meta = await self.client.get("/test9")
        self.assertEqual(value, b'9')
        self.assertIsNotNone(meta)

        keys_list = await self.client.range_keys(key_range=range_all())
        self.assertEqual(len(keys_list), 10)

        value_list = await self.client.range(key_range=range_all())
        self.assertEqual(len(value_list), 10)
        value = [str(v[0]) for v in value_list]
        real_value = [str(i) for i in range(0, 10)]
        self.assertEqual(value.sort(), real_value)

        value_list = await self.client.range(key_range=range_all(), limit=5)
        self.assertEqual(len(value_list), 5)

        value_list = await self.client.range(key_range=range_prefix('/'))
        self.assertEqual(len(value_list), 10)

        value_list = await self.client.range(key_range=range_prefix('/'), limit=11)
        self.assertEqual(len(value_list), 10)

        value_list = await self.client.range(key_range=range_greater('/test8'))
        self.assertEqual(len(value_list), 2)
        self.assertEqual(value_list[0][0], b'8')
        self.assertEqual(value_list[1][0], b'9')

        value_list = await self.client.range(key_range=range_greater('/test10'))
        self.assertEqual(len(value_list), 0)

        await self.client.delete(key_range='/test9')
        value, meta = await self.client.get("/test9")
        self.assertIsNone(value)
        self.assertIsNone(meta)

        value_list = await self.client.pop(key_range='/test8')
        self.assertEqual(len(value_list), 1)
        self.assertEqual(value_list[0][0], b'8')

        value_list = await self.client.delete(key_range=range_prefix('/'))
        self.assertEqual(len(value_list), 8)

    @asynctest
    async def test_transaction(self):

        await self.client.put('/trans1', 'trans1')
        await self.client.put('/trans2', 'trans2')

        is_success, response = await self.client.txn(compare=[
            transaction.Value('/trans1') == b'trans1',
            transaction.Value('/trans2') == b'trans2'
        ], success=[
            KV.delete.txn('/trans1'),
            KV.put.txn('/trans3', 'trans3', prev_kv=True)
        ], fail=[
            KV.delete.txn('/trans1')
        ])

        if is_success:
            self.assertEqual(len(response), 2)
            del_response = response[0]
            self.assertIsNone(del_response[0])
            self.assertIsNone(del_response[1])
            put_response = response[1]
            self.assertIsNotNone(put_response[0])
            self.assertIsNotNone(put_response[1])
        else:
            self.assertEqual(len(response), 1)
            del_response = response[0]
            self.assertIsNone(del_response[0])
            self.assertIsNone(del_response[1])






if __name__ == '__main__':
    unittest.main()

