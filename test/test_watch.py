import unittest
import functools
import asyncio

from aioetcd3.client import client
from aioetcd3.help import range_all
from aioetcd3.watch import EVENT_TYPE_CREATE,EVENT_TYPE_DELETE,EVENT_TYPE_PUT


def asynctest(f):
    @functools.wraps(f)
    def _f(self):
        return asyncio.get_event_loop().run_until_complete(f(self))

    return _f


class WatchTest(unittest.TestCase):
    def setUp(self):
        endpoints = "127.0.0.1:2379"
        self.client = client(endpoint=endpoints)
        self.tearDown()

    @unittest.SkipTest
    @asynctest
    async def test_watch_1(self):
        f1 = asyncio.get_event_loop().create_future()
        async def watch_1():
            i = 0
            async with self.client.watch_scope('/foo') as response:
                f1.set_result(None)
                async for event, meta in response:
                    i = i + 1
                    if i == 1:
                        self.assertEqual(event.type, EVENT_TYPE_CREATE)
                        self.assertEqual(event.key, b'/foo')
                        self.assertEqual(event.value, b'foo')
                    elif i == 2:
                        self.assertEqual(event.type, EVENT_TYPE_PUT)
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
            async for event, meta in self.client.watch('/foo', prev_kv=True, create_event=True):
                if event is None:
                    f2.set_result(None)
                    continue

                i = i + 1
                if i == 1:
                    self.assertEqual(event.type, EVENT_TYPE_CREATE)
                    self.assertEqual(event.key, b'/foo')
                    self.assertEqual(event.value, b'foo')
                elif i == 2:
                    self.assertEqual(event.type, EVENT_TYPE_PUT)
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
            async for event, meta in self.client.watch('/foo', prev_kv=True, noput=True, create_event=True):
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
            async for event, meta in self.client.watch('/foo', prev_kv=True, nodelete=True, create_event=True):
                if event is None:
                    f4.set_result(None)
                    continue

                i = i + 1
                if i == 1:
                    self.assertEqual(event.type, EVENT_TYPE_CREATE)
                    self.assertEqual(event.key, b'/foo')
                    self.assertEqual(event.value, b'foo')
                elif i == 2:
                    self.assertEqual(event.type, EVENT_TYPE_PUT)
                    self.assertEqual(event.key, b'/foo')
                    self.assertEqual(event.value, b'foo1')
                    self.assertEqual(event.pre_value, b'foo')
                    break

        try:
            w1 = asyncio.ensure_future(watch_1())
            w2 = asyncio.ensure_future(watch_2())
            w3 = asyncio.ensure_future(watch_3())
            w4 = asyncio.ensure_future(watch_4())

            await asyncio.wait([f1, f2, f3, f4])

            await self.client.put('/foo', 'foo')
            await self.client.put('/foo', 'foo1')
            await self.client.delete('/foo')

            done, pending = await asyncio.wait([w1, w2, w3, w4], timeout=20)
            for t in done:
                t.result()
        finally:
            await self.client.stop_task()

    @asynctest
    async def tearDown(self):
        await self.client.delete(range_all())
