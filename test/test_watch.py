import unittest
import functools
import asyncio
from grpc import RpcError

from aioetcd3.client import client
from aioetcd3.help import range_all, range_prefix
from aioetcd3.watch import EVENT_TYPE_CREATE,EVENT_TYPE_DELETE,EVENT_TYPE_MODIFY,\
    CompactRevisonException, WatchException


def asynctest(f):
    @functools.wraps(f)
    def _f(self):
        return asyncio.get_event_loop().run_until_complete(f(self))

    return _f


class WatchTest(unittest.TestCase):
    @asynctest
    async def setUp(self):
        self.endpoints = "127.0.0.1:2379"
        self.client = client(endpoint=self.endpoints)
        await self.cleanUp()

    @asynctest
    async def test_watch_1(self):
        f1 = asyncio.get_event_loop().create_future()
        async def watch_1():
            i = 0
            async with self.client.watch_scope('/foo') as response:
                f1.set_result(None)
                async for event in response:
                    i = i + 1
                    if i == 1:
                        self.assertEqual(event.type, EVENT_TYPE_CREATE)
                        self.assertEqual(event.key, b'/foo')
                        self.assertEqual(event.value, b'foo')
                    elif i == 2:
                        self.assertEqual(event.type, EVENT_TYPE_MODIFY)
                        self.assertEqual(event.key, b'/foo')
                        self.assertEqual(event.value, b'foo1')
                    elif i == 3:
                        self.assertEqual(event.type, EVENT_TYPE_DELETE)
                        self.assertEqual(event.key, b'/foo')
                        # delete event has no value
                        # self.assertEqual(event.value, b'foo1')
                        break

        f2 = asyncio.get_event_loop().create_future()
        async def watch_2():
            i = 0
            async for event in self.client.watch('/foo', prev_kv=True, create_event=True):
                if event is None:
                    f2.set_result(None)
                    continue

                i = i + 1
                if i == 1:
                    self.assertEqual(event.type, EVENT_TYPE_CREATE)
                    self.assertEqual(event.key, b'/foo')
                    self.assertEqual(event.value, b'foo')
                elif i == 2:
                    self.assertEqual(event.type, EVENT_TYPE_MODIFY)
                    self.assertEqual(event.key, b'/foo')
                    self.assertEqual(event.value, b'foo1')
                    self.assertEqual(event.pre_value, b'foo')
                elif i == 3:
                    self.assertEqual(event.type, EVENT_TYPE_DELETE)
                    self.assertEqual(event.key, b'/foo')
                    # self.assertEqual(event.value, b'foo1')
                    break

        f3 = asyncio.get_event_loop().create_future()
        async def watch_3():
            i = 0
            async for event in self.client.watch('/foo', prev_kv=True, noput=True, create_event=True):
                if event is None:
                    f3.set_result(None)
                    continue

                i = i + 1
                if i == 1:
                    self.assertEqual(event.type, EVENT_TYPE_DELETE)
                    self.assertEqual(event.key, b'/foo')
                    # self.assertEqual(event.value, b'foo1')
                    break

        f4 = asyncio.get_event_loop().create_future()
        async def watch_4():
            i = 0
            async for event in self.client.watch('/foo', prev_kv=True, nodelete=True, create_event=True):
                if event is None:
                    f4.set_result(None)
                    continue

                i = i + 1
                if i == 1:
                    self.assertEqual(event.type, EVENT_TYPE_CREATE)
                    self.assertEqual(event.key, b'/foo')
                    self.assertEqual(event.value, b'foo')
                elif i == 2:
                    self.assertEqual(event.type, EVENT_TYPE_MODIFY)
                    self.assertEqual(event.key, b'/foo')
                    self.assertEqual(event.value, b'foo1')
                    self.assertEqual(event.pre_value, b'foo')
                    break

        w1 = asyncio.ensure_future(watch_1())
        w2 = asyncio.ensure_future(watch_2())
        w3 = asyncio.ensure_future(watch_3())
        w4 = asyncio.ensure_future(watch_4())

        await asyncio.wait_for(asyncio.wait([f1, f2, f3, f4]), 2)

        await self.client.put('/foo', 'foo')
        await self.client.put('/foo', 'foo1')
        await self.client.delete('/foo')

        done, pending = await asyncio.wait([w1, w2, w3, w4], timeout=20)
        for t in done:
            t.result()

    @asynctest
    async def test_watch_reconnect(self):
        f1 = asyncio.get_event_loop().create_future()
        f2 = asyncio.get_event_loop().create_future()
        async def watch_1():
            i = 0
            async with self.client.watch_scope('/foo') as response:
                f1.set_result(None)
                async for event in response:
                    i = i + 1
                    if i == 1:
                        self.assertEqual(event.type, EVENT_TYPE_CREATE)
                        self.assertEqual(event.key, b'/foo')
                        self.assertEqual(event.value, b'foo')
                        f2.set_result(None)
                    elif i == 2:
                        self.assertEqual(event.type, EVENT_TYPE_MODIFY)
                        self.assertEqual(event.key, b'/foo')
                        self.assertEqual(event.value, b'foo1')
                    elif i == 3:
                        self.assertEqual(event.type, EVENT_TYPE_DELETE)
                        self.assertEqual(event.key, b'/foo')
                        # delete event has no value
                        # self.assertEqual(event.value, b'foo1')
                        break
        t1 = asyncio.ensure_future(watch_1())
        await f1
        await self.client.put('/foo', 'foo')
        await f2
        self.client.update_server_list(self.endpoints)
        await self.client.put('/foo', 'foo1')
        await self.client.delete('/foo')
        await t1
    
    @asynctest
    async def test_watch_create_cancel(self):
        async def watch_1():
            async with self.client.watch_scope('/foo') as _:
                pass
        async def watch_2():
            async with self.client.watch_scope('/foo') as _:
                await asyncio.sleep(5)
        for _ in range(0, 5):
            watches = [asyncio.ensure_future(watch_1() if i % 2 else watch_2()) for i in range(0, 200)]
            await asyncio.sleep(1)
            for w in watches[::3]:
                w.cancel()
            self.client.update_server_list(self.endpoints)
            await asyncio.sleep(0.01)
            for w in watches[1::3]:
                w.cancel()
            await asyncio.sleep(0.3)
            for w in watches[2::3]:
                w.cancel()
            await asyncio.wait_for(asyncio.wait(watches), 3)
            results = await asyncio.gather(*watches, return_exceptions=True)
            print("Finished:", len([r for r in results if r is None]), "Cancelled:", len([r for r in results if r is not None]))
            self.assertIsNotNone(self.client._watch_task_running)
        await asyncio.sleep(3)
        self.assertIsNone(self.client._watch_task_running)
    
    @asynctest
    async def test_batch_events(self):
        f1 = asyncio.get_event_loop().create_future()
        f2 = asyncio.get_event_loop().create_future()
        def _check_event(e, criterias):
            if criterias[0]:
                self.assertEqual(e.type, criterias[0])
            if criterias[1]:
                self.assertEqual(e.key, criterias[1])
            if criterias[2]:
                self.assertEqual(e.value, criterias[2])
        async def watch_1():
            asserts = [(EVENT_TYPE_CREATE, b'/foo/1', b'1'),
                       (EVENT_TYPE_CREATE, b'/foo/2', b'2'),
                       (EVENT_TYPE_MODIFY, b'/foo/1', b'2'),
                       (EVENT_TYPE_MODIFY, b'/foo/2', b'3'),
                       (EVENT_TYPE_DELETE, b'/foo/1',None),
                       (EVENT_TYPE_DELETE, b'/foo/2',None)]
            async with self.client.watch_scope(range_prefix('/foo/')) as response:
                f1.set_result(None)
                async for e in response:
                    _check_event(e, asserts.pop(0))
                    if not asserts:
                        break
        
        async def watch_2():
            asserts = [((EVENT_TYPE_CREATE, b'/foo/1', b'1'),
                       (EVENT_TYPE_CREATE, b'/foo/2', b'2'),),
                       ((EVENT_TYPE_MODIFY, b'/foo/1', b'2'),),
                       ((EVENT_TYPE_MODIFY, b'/foo/2', b'3'),),
                       ((EVENT_TYPE_DELETE, b'/foo/1',None),
                       (EVENT_TYPE_DELETE, b'/foo/2',None))]
            async with self.client.watch_scope(range_prefix('/foo/'), batch_events=True)\
                    as response:
                f2.set_result(None)
                async for es in response:
                    batch = asserts.pop(0)
                    self.assertEqual(len(es), len(batch))
                    for e, a in zip(es, batch):
                        _check_event(e, a)
                    if not asserts:
                        break
        
        t1 = asyncio.ensure_future(watch_1())
        t2 = asyncio.ensure_future(watch_2())
        await asyncio.wait_for(asyncio.wait([f1,f2]), 2)
        self.assertTrue((await self.client.txn([],[self.client.put.txn('/foo/1', '1'),
                                  self.client.put.txn('/foo/2', '2')],[]))[0])
        await self.client.put('/foo/1', '2')
        await self.client.put('/foo/2', '3')
        self.assertTrue((await self.client.txn([],[self.client.delete.txn('/foo/1'),
                                  self.client.delete.txn('/foo/2')],[]))[0])        
        await asyncio.gather(t1, t2)
    
    @asynctest
    async def test_compact_revision(self):
        await self.client.put('/foo', '1')
        first_revision = self.client.last_response_info.revision
        await self.client.put('/foo', '2')
        await self.client.put('/foo', '3')
        await self.client.put('/foo', '4')
        await self.client.put('/foo', '5')
        compact_revision = self.client.last_response_info.revision
        await self.client.compact(compact_revision, True)
        async def watch_1():
                async with self.client.watch_scope('/foo', start_revision=first_revision) as response:
                    with self.assertRaises(CompactRevisonException) as cm:
                        async for e in response:
                            raise ValueError("Not raised")
                    self.assertEqual(cm.exception.revision, compact_revision)
        
        async def watch_2():
            async with self.client.watch_scope('/foo', ignore_compact=True, start_revision=first_revision) as responses:
                async for e in responses:
                    self.assertEqual(e.type, EVENT_TYPE_MODIFY)
                    self.assertEqual(e.key, b'/foo')
                    self.assertEqual(e.value, b'5')
                    self.assertEqual(e.revision, compact_revision)
                    break
        await watch_1()
        await watch_2()

    @asynctest
    async def test_watch_exception(self):
        f1 = asyncio.get_event_loop().create_future()
        f2 = asyncio.get_event_loop().create_future()
        async def watch_1():
            i = 0
            async with self.client.watch_scope('/foo') as response:
                f1.set_result(None)
                with self.assertRaises(WatchException):
                    async for event in response:
                        i = i + 1
                        if i == 1:
                            self.assertEqual(event.type, EVENT_TYPE_CREATE)
                            self.assertEqual(event.key, b'/foo')
                            self.assertEqual(event.value, b'foo')
                            f2.set_result(None)
                        elif i == 2:
                            raise ValueError("Not raised")
        f3 = asyncio.get_event_loop().create_future()
        f4 = asyncio.get_event_loop().create_future()
        async def watch_2():
            i = 0
            async with self.client.watch_scope('/foo', always_reconnect=True) as response:
                f3.set_result(None)
                async for event in response:
                    i = i + 1
                    if i == 1:
                        self.assertEqual(event.type, EVENT_TYPE_CREATE)
                        self.assertEqual(event.key, b'/foo')
                        self.assertEqual(event.value, b'foo')
                        f4.set_result(None)
                    elif i == 2:
                        self.assertEqual(event.type, EVENT_TYPE_MODIFY)
                        self.assertEqual(event.key, b'/foo')
                        self.assertEqual(event.value, b'foo1')
                    elif i == 3:
                        self.assertEqual(event.type, EVENT_TYPE_DELETE)
                        self.assertEqual(event.key, b'/foo')
                        # delete event has no value
                        # self.assertEqual(event.value, b'foo1')
                        break
        t1 = asyncio.ensure_future(watch_1())
        t2 = asyncio.ensure_future(watch_2())
        await f1
        await f3
        await self.client.put('/foo', 'foo')
        await f2
        await f4
        fake_endpoints = 'ipv4:///127.0.0.1:49999'
        self.client.update_server_list(fake_endpoints)
        await asyncio.sleep(2)
        self.client.update_server_list(self.endpoints)
        await self.client.put('/foo', 'foo1')
        await self.client.delete('/foo')
        await t1
        await t2
    
    @asynctest
    async def tearDown(self):
        await self.cleanUp()
        await self.client.close()
        
    async def cleanUp(self):
        await self.client.delete(range_all())


if __name__ == '__main__':
    unittest.main()
