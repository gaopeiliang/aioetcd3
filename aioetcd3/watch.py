import asyncio

from aioetcd3.base import StubMixin
from aioetcd3._etcdv3 import rpc_pb2 as rpc
from aioetcd3._etcdv3 import kv_pb2 as kv
from asyncio.queues import Queue
from aioetcd3.utils import put_key_range
from aioetcd3.kv import KVMetadata
import aioetcd3._etcdv3.rpc_pb2_grpc as stub

DEFAUTL_INPUT_QUEUE_LIMIT = 10000
EVENT_TYPE_PUT = kv.Event.PUT
EVENT_TYPE_DELETE = kv.Event.DELETE
EVENT_TYPE_CREATE = kv.Event.DELETE + 1


class WatchResponseMeta(object):
    def __init__(self, response):
        self.watch_id = response.watch_id
        self.created = response.created
        self.canceled = response.canceled
        self.compact_revision = response.compact_revision
        self.cancel_reason = response.cancel_reason

    def __str__(self):
        return str({"watch_id": self.watch_id, "created": self.created,
                    "canceled": self.canceled, "compact_revision": self.compact_revision,
                    "cancel_reason": self.cancel_reason})


class Event(object):
    def __init__(self, event):
        self.type = event.type

        if self.type == kv.Event.PUT and event.kv.version == 1:
            self.type = EVENT_TYPE_CREATE

        self.key = event.kv.key
        self.value = event.kv.value
        self.meta = KVMetadata(event.kv)

        self.pre_value = event.prev_kv.value
        self.pre_meta = KVMetadata(event.prev_kv)

    def __str__(self):
        return f'{self.type} {self.key},{self.value}'

        
class WatchScope(object):
    def __init__(self, _iter):
        self._iter = _iter
    async def __aenter__(self):
        await self._iter.__anext__()
        return self._iter
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._iter.aclose()


class Watch(StubMixin):
    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._loop = channel._loop

        if hasattr(self, '_has_create_stream') and self._has_create_stream:
            # this means stream api has created , recreated it
            asyncio.ensure_future(self.stop_stream_task(self._back_stream_task))
            self._has_create_stream = False
            self.create_stream_task()

        self._watch_stub = stub.WatchStub(channel)

        self._input_queue = Queue(maxsize=DEFAUTL_INPUT_QUEUE_LIMIT)
        self._pending_request_queue = Queue(maxsize=1)
        self._watch_listening = {}
        self._watch_id = {}

    async def stop_stream_task(self, task):
        task.cancel()

    def create_stream_task(self):
        self._input_queue = Queue()
        self._input_request = self.arequest(self._input_queue)

        self._back_stream_task = asyncio.ensure_future(self.stream_task(self._watch_stub,
                                                                        self._input_request))

        self._has_create_stream = True

    async def stream_task(self, stub, request_iter):
        async with stub.Watch.with_scope(request_iter) as response_iter:
            async for response in response_iter:
                if response.created:
                    request, out_queue, future = await self._pending_request_queue.get()
                    self._watch_listening[response.watch_id] = (request, out_queue)
                    self._watch_id[out_queue] = response.watch_id

                    future.set_result(None)
                    # send out queue None event , indicate stream task has been created
                    await out_queue.put((None, None))

                if response.watch_id in self._watch_listening:
                    _, out_q = self._watch_listening[response.watch_id]

                    await out_q.put((response.events, WatchResponseMeta(response)))

                if response.canceled:
                    if response.watch_id in self._watch_listening:
                        _, _, future = await self._pending_request_queue.get()
                        await self._watch_listening[response.watch_id][1].put((None, WatchResponseMeta(response)))

                        _, out_q = self._watch_listening[response.watch_id]

                        if out_q in self._watch_id:
                            del self._watch_id[out_q]

                        del self._watch_listening[response.watch_id]
                        future.set_result(None)

    async def arequest(self, input_queue):
        while True:
            request, out_queue, future = await input_queue.get()
            if request is None and out_queue is None:
                # stop iter request , stop watch stream
                break
            # self._pending_created.append((request, out_queue))
            await self._pending_request_queue.put((request, out_queue, future))
            yield request
            await future

    async def watch(self, key_range, start_revision=None, noput=False, nodelete=False, prev_kv=False,
                    create_event=False):

        filter = []
        if noput:
            filter.append(rpc.WatchCreateRequest.NOPUT)
        if nodelete:
            filter.append(rpc.WatchCreateRequest.NODELETE)

        watch_request = rpc.WatchCreateRequest(start_revision=start_revision,
                                               filters=filter,
                                               prev_kv=prev_kv)

        put_key_range(watch_request, key_range)
        request = rpc.WatchRequest(create_request=watch_request)

        if not hasattr(self, '_has_create_stream') and not self._has_create_stream:
            # this means stream api has not start
            self.create_stream_task()

        done_future = self._loop.create_future()
        response_queue = Queue()
        await self._input_queue.put((request, response_queue, done_future))
        await done_future
        
        try:
            await response_queue.get()
            if create_event:
                yield (None, None)

            while True:
                events, meta = await  response_queue.get()
                if events is None:
                    break

                for e in events:
                    yield (Event(e), meta)
        finally:
            watch_id = self._watch_id[response_queue]
            cancel_request = rpc.WatchRequest(cancel_request=rpc.WatchCancelRequest(watch_id=watch_id))
            done_future = self._loop.create_future()
            await self._input_queue.put((cancel_request, response_queue, done_future))
            await done_future

            if not self._watch_id:
                # this means has no stream request , stop stream api
                self._has_create_stream = False
                await self.stop_stream_task(self._back_stream_task)

    def watch_scope(self, key_range, start_revision=None, noput=False, nodelete=False, prev_kv=False):
        return WatchScope(self.watch(key_range, start_revision=start_revision,
                                     noput=noput, nodelete=nodelete, prev_kv=prev_kv, create_event=True))
