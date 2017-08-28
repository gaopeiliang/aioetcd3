import asyncio

from aioetcd3.base import StubMixin
import aioetcd3.rpc.rpc_pb2 as rpc
import aioetcd3.rpc.kv_pb2 as kv
from asyncio.queues import Queue
from aioetcd3.utils import put_key_range
from aioetcd3.kv import KVMetadata

DEFAUTL_INPUT_QUEUE_LIMIT = 10000


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

        if self.type == kv.Event.EventType.PUT and event.kv.Version == 1:
            self.type = 'created'

        self.key = event.kv.key
        self.value = event.kv.value
        self.meta = KVMetadata(event.kv)

        self.pre_value = event.prev_kv.value
        self.pre_meta = KVMetadata(event.prev_kv)

    def __str__(self):
        return f'{self.type} {self.key},{self.value}'

class Watch(StubMixin):
    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._watch_stub = rpc.WatchStub(channel)

        if hasattr(self, 'input_queue'):
            # stream task mybe running , stop it first!
            stop = asyncio.ensure_future(self.stop_task())
            asyncio.wait(stop)

        self._input_queue = Queue(maxsize=DEFAUTL_INPUT_QUEUE_LIMIT)
        # every request. unique Queue append here, correspond with response
        self._pending_created = []
        self._watch_listening = {}
        self._watch_id = {}

        self.stream_task = asyncio.ensure_future(self.stream_task(self.arequest()))

    async def stream_task(self, request_iter):
        async with self._watch_stub.Watch.with_scope(request_iter) as response_iter:

            async for response in response_iter:
                if response.created:
                    request, out_queue = self._pending_created.pop(0)
                    self._watch_listening[response.watch_id] = (request, out_queue)
                    self._watch_id[out_queue] = response.watch_id

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

    async def arequest(self):
        if self._watch_listening:
            l = self._watch_listening
            self._watch_listening = {}
            for request, out_queue in l.items():
                self._pending_created.append((request, out_queue))
                yield request

        while True:
            is_created, request, out_queue = await self._input_queue.get()
            if request is None and out_queue is None:
                # stop iter request , stop watch stream
                break
            if is_created:
                self._pending_created.append((request, out_queue))

            yield request

    async def stop_task(self):
        await self._input_queue.put((None, None, None))

    async def watch(self, key_range, start_revision=None, noput=False, nodelete=False, prev_kv=False):

        filter = []
        if noput:
            filter.append(rpc.WatchCreateRequest.FilterType.NOPUT)
        if nodelete:
            filter.append(rpc.WatchCreateRequest.FilterType.NODELETE)

        watch_request = rpc.WatchCreateRequest(start_revision=start_revision,
                                               filters=filter,
                                               prev_kv=prev_kv)

        put_key_range(watch_request, key_range)
        request = rpc.WatchRequest(create_request=watch_request)

        response_queue = Queue()
        await self._input_queue.put((True, request, response_queue))

        while True:
            events, meta = await  response_queue.get()
            if events is None:
                break

            for e in events:
                yield (Event(e), meta)

        # async def event_iter(queue):
        #     while True:
        #         event, meta = await queue.get()
        #         if event is None and meta is None:
        #             break
        #         yield event
        #
        # await self._input_queue.put((watch_request, response_queue))
        #
        # return response_queue, event_iter(response_queue)

    # async def unwatch(self, unique_queue):
    #
    #     max_time = 5
    #     while True:
    #         queue_info = [q for _, q in self._pending_created]
    #         if unique_queue not in queue_info:
    #             break
    #         else:
    #             max_time = max_time - 1
    #             await asyncio.sleep(1)
    #
    #         if max_time <= 0:
    #             raise ValueError("watch request mybe have no response")
    #
    #     if unique_queue not in self._watch_id:
    #         raise ValueError("find watch id error")
    #
    #     watch_id = self._watch_id[unique_queue]
    #
    #     request = rpc.WatchCancelRequest(watch_id=watch_id)
    #     response_queue = Queue()
    #
    #     await self._input_queue.put((request, response_queue))
