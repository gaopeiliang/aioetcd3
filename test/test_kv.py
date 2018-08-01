import unittest
import asyncio
import functools
from aioetcd3.client import client
from aioetcd3.kv import KV
from aioetcd3.help import range_all, range_prefix, range_greater, range_greater_equal
from aioetcd3 import transaction


def asynctest(f):
    @functools.wraps(f)
    def _f(self):
        return asyncio.get_event_loop().run_until_complete(f(self))

    return _f


class KVTest(unittest.TestCase):
    
    @asynctest
    async def setUp(self):
        endpoints = "127.0.0.1:2379"
        self.client = client(endpoint=endpoints)
        endpoints = "127.0.0.1:2379"
        self.client.update_server_list(endpoint=endpoints)
        await self.cleanUp()
    
    async def cleanUp(self):
        await self.client.delete(key_range=range_all())
    
    @asynctest
    async def tearDown(self):
        await self.cleanUp()
        await self.client.close()

    @asynctest
    async def test_put_get(self):

        for i in range(0, 10):
            key = '/test' + str(i)
            value, meta = await self.client.put(key, str(i))
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
        value = [v[1].decode('utf-8') for v in value_list]
        value.sort()
        real_value = [str(i) for i in range(0, 10)]
        self.assertEqual(value, real_value)

        value_list = await self.client.range(key_range=range_all(), limit=5)
        self.assertEqual(len(value_list), 5)

        value_list = await self.client.range(key_range=range_prefix('/'))
        self.assertEqual(len(value_list), 10)

        value_list = await self.client.range(key_range=range_prefix('/'), limit=11)
        self.assertEqual(len(value_list), 10)

        value_list = await self.client.range(key_range=range_greater_equal('/test8'))
        self.assertEqual(len(value_list), 2)
        self.assertEqual(value_list[0][1], b'8')
        self.assertEqual(value_list[1][1], b'9')

        value_list = await self.client.range(key_range=range_greater('/testa'))
        self.assertEqual(len(value_list), 0)

        await self.client.delete(key_range='/test9')
        value, meta = await self.client.get("/test9")
        self.assertIsNone(value)
        self.assertIsNone(meta)

        value_list = await self.client.pop(key_range='/test8')
        self.assertEqual(len(value_list), 1)
        self.assertEqual(value_list[0][0], b'/test8')
        self.assertEqual(value_list[0][1], b'8')

        value_list = await self.client.delete(key_range=range_prefix('/'), prev_kv=True)
        self.assertEqual(len(value_list), 8)

    @asynctest
    async def test_transaction(self):
        await self.client.put('/trans1', 'trans1')
        await self.client.put('/trans2', 'trans2')
        
        is_success, response = await self.client.txn(compare=[
            transaction.Value('/trans1') == b'trans1',
            transaction.Value('/trans2') == b'trans2'
        ], success=[
            KV.get.txn('/trans1'),
            KV.range.txn('/trans2')
        ], fail=[
            KV.delete.txn('/trans1')
        ])

        self.assertEqual(is_success, True)
        self.assertEqual(len(response), 2)
        
        self.assertEqual(response[0][0], b'trans1')
        self.assertEqual(response[1][0][:2], (b'/trans2', b'trans2'))
        
        is_success, response = await self.client.txn(compare=[
            transaction.Value('/trans1') == b'trans1',
            transaction.Value('/trans2') == b'trans2'
        ], success=[
            KV.delete.txn('/trans1'),
            KV.put.txn('/trans2', 'trans2', prev_kv=True),
            KV.put.txn('/trans3', 'trans3', prev_kv=True)
        ], fail=[
            KV.delete.txn('/trans1')
        ])

        self.assertEqual(is_success, True)
        self.assertEqual(len(response), 3)
        del_response = response[0]
        self.assertEqual(del_response, 1)
        put_response = response[1]
        self.assertEqual(put_response[0], b'trans2')
        put_response = response[2]
        # there is not pre_kv None
        self.assertIsNone(put_response[0])

        is_success, response = await self.client.txn(compare=[
            transaction.Value('/trans3') != b'trans3',
            transaction.Version('/trans3') < 1000,
            transaction.Mod('/trans3') > 100,
            transaction.Create('/trans3') != 200
        ], success=[
        ], fail=[
            KV.delete.txn('/trans3', prev_kv=True)
        ])

        self.assertEqual(is_success, False)
        self.assertEqual(len(response), 1)
        self.assertEqual(len(response[0]), 1)
        self.assertEqual(response[0][0][:2], (b'/trans3', b'trans3'))

if __name__ == '__main__':
    unittest.main()

