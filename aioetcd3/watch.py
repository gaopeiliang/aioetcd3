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
        print("---- after scope anext ----")
        return self._iter
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._iter.aclose()


class Watch(StubMixin):
    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._watch_stub = stub.WatchStub(channel)
        self._loop = channel._loop

        if hasattr(self, '_input_queue'):
            # stream task mybe running , stop it first!
            asyncio.ensure_future(self.stop_task())

        self._input_queue = Queue(maxsize=DEFAUTL_INPUT_QUEUE_LIMIT)
        # every request. unique Queue append here, correspond with response
        self._pending_created = []
        self._watch_listening = {}
        self._watch_id = {}

        # self._exit_future = self._loop.create_future()
        self._input_request = self.arequest(self._input_queue)
        self._back_stream_task = asyncio.ensure_future(self.stream_task(self._watch_stub, self._input_request))

    async def stream_task(self, stub, request_iter):
        async with stub.Watch.with_scope(request_iter) as response_iter:

            async for response in response_iter:
                if response.created:
                    request, out_queue = self._pending_created.pop(0)
                    self._watch_listening[response.watch_id] = (request, out_queue)
                    self._watch_id[out_queue] = response.watch_id

                    # send out queue None event , indicate stream task has been created
                    await out_queue.put((None, None))

                if response.watch_id in self._watch_listening:
                    _, out_q = self._watch_listening[response.watch_id]

                    await out_q.put((response.events, WatchResponseMeta(response)))

                if response.canceled:
                    if response.watch_id in self._watch_listening:
                        await self._watch_listening[response.watch_id][1].put((None, WatchResponseMeta(response)))

                        _, out_q = self._watch_listening[response.watch_id]

                        if out_q in self._watch_id:
                            del self._watch_id[out_q]

                        del self._watch_listening[response.watch_id]

    async def arequest(self, input_queue):
        if self._watch_listening:
            l = self._watch_listening
            self._watch_listening = {}
            for request, out_queue in l.items():
                self._pending_created.append((request, out_queue))
                yield request

        while True:
            is_created, request, out_queue = await input_queue.get()
            if request is None and out_queue is None:
                # stop iter request , stop watch stream
                break
            if is_created:
                self._pending_created.append((request, out_queue))

            yield request

    def stop_task(self):
        if hasattr(self, '_last_stop_task'):
            last_task = self._last_stop_task
        else:
            last_task = None
        t = self._stop_task(self._input_queue, self._back_stream_task, last_task)
        self._last_stop_task = t
        return t

    @staticmethod
    async def _stop_task(input_queue, exit_future, last_task):
        if last_task is not None:
            try:
                await last_task
            except Exception:
                pass
        await input_queue.put((None, None, None))
        exit_future.cancel()
        try:
            await exit_future
        except asyncio.CancelledError:
            pass

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

        response_queue = Queue()
        await self._input_queue.put((True, request, response_queue))
        
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
            await self._input_queue.put((False, cancel_request, response_queue))
    
    def watch_scope(self, key_range, start_revision=None, noput=False, nodelete=False, prev_kv=False):
        return WatchScope(self.watch(key_range, start_revision=start_revision,
                                     noput=noput, nodelete=nodelete, prev_kv=prev_kv, create_event=True))
