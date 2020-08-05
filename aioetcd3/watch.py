import asyncio

from asyncio import CancelledError
from aioetcd3.base import StubMixin
from aioetcd3._etcdv3 import rpc_pb2 as rpc
from aioetcd3._etcdv3 import kv_pb2 as kv
from asyncio.queues import Queue, QueueEmpty, QueueFull
from aioetcd3.utils import put_key_range
from aioetcd3.kv import KVMetadata
import aioetcd3._etcdv3.rpc_pb2_grpc as stub

EVENT_TYPE_MODIFY = "MODIFY"
EVENT_TYPE_DELETE = "DELETE"
EVENT_TYPE_CREATE = "CREATE"


class Event(object):
    def __init__(self, event, revision):
        if event.type == kv.Event.PUT:
            if event.kv.version == 1:
                self.type = EVENT_TYPE_CREATE
            else:
                self.type = EVENT_TYPE_MODIFY
        else:
            self.type = EVENT_TYPE_DELETE

        self.key = event.kv.key
        self.value = event.kv.value
        self.meta = KVMetadata(event.kv)

        self.pre_value = event.prev_kv.value
        self.pre_meta = KVMetadata(event.prev_kv)
        
        self.revision = revision

    def is_put(self):
        return self.type == EVENT_TYPE_CREATE or self.type == EVENT_TYPE_MODIFY

    def __str__(self):
        return f'{self.type} {self.key},{self.value}'

        
class WatchScope(object):
    def __init__(self, _iter):
        self._iter = _iter

    async def __aenter__(self):
        await self._iter.__anext__()
        return self._iter

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            await self._iter.aclose()
        except GeneratorExit:
            pass


class _Pipe(object):
    """
    Selectable asyncio channel
    """
    def __init__(self, maxsize=None, *, loop=None):
        self._loop = loop
        self._notify = asyncio.Event(loop=loop)
        self._full_notify = asyncio.Event(loop=loop)
        self._queue = []
        self._full_notify.set()
        if maxsize is None or maxsize <= 0:
            self._maxsize = None
        else:
            self._maxsize = maxsize
        self._last_watch_version = None
    
    def is_empty(self):
        return not self._notify.is_set()
    
    def is_full(self):
        return not self._full_notify.is_set()
    
    async def put(self, value):
        await self.wait_full()
        self.put_nowait(value)
    
    def put_nowait(self, value):
        if self.is_full():
            raise QueueFull
        self._queue.append(value)
        if self.is_empty():
            self._notify.set()
        if self._maxsize and len(self._queue) >= self._maxsize:
            self._full_notify.clear()        
    
    async def get(self, value):
        await self.wait_empty()
        return self.get_nowait()
    
    def get_nowait(self):
        if self.is_empty():
            raise QueueEmpty
        if self._maxsize or len(self._queue) <= self._maxsize:
            self._full_notify.set()
        if len(self._queue) == 1:
            self._notify.clear()
        return self._queue.pop(0)
    
    async def read(self, limit=None):
        await self.wait_empty()
        return self.read_nowait(limit)
    
    def read_nowait(self, limit=None):
        if self.is_empty():
            raise QueueEmpty
        if limit is None or limit <= 0:
            read_size = len(self._queue)
        else:
            read_size = min(len(self._queue), limit)
        result = self._queue[:read_size]
        del self._queue[:read_size]
        if not self._maxsize or len(self._queue) < self._maxsize:
            self._full_notify.set()
        if len(self._queue) == 0:
            self._notify.clear()
        return result
    
    async def write(self, values):
        await self.wait_full()
        return self.write_nowait(values)
    
    def write_nowait(self, values):
        if self.is_full():
            raise QueueFull
        if self._maxsize is None:
            write_size = len(values)
        else:
            write_size = min(len(values), self._maxsize - len(self._queue))
        self._queue.extend(values[:write_size])
        if len(self._queue) > 0:
            self._notify.set()
        if self._maxsize and len(self._queue) >= self._maxsize:
            self._full_notify.clear()
        return write_size
    
    async def wait_full(self):
        while self.is_full():
            await self._full_notify.wait()
        
    async def wait_empty(self):
        while self.is_empty():
            await self._notify.wait()


async def _select(pipes, futures, *, loop=None):
    futures = [asyncio.ensure_future(f, loop=loop) for f in futures]
    _, pending = await asyncio.wait([p.wait_empty() for p in pipes] + list(futures),
                        loop=loop, return_when=asyncio.FIRST_COMPLETED)
    for p in pending:
        if p not in futures:
            p.cancel()
            try:
                await p
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
    return [p for p in pipes if not p.is_empty()], [f for f in futures if f.done()]


class WatchException(Exception):
    def _clone(self):
        return type(self)(*self.args)

class CompactRevisonException(WatchException):
    def __init__(self, revision):
        super().__init__(f"Watch on compact revision. Min revision is {revision}")
        self.revision = revision
    
    def _clone(self):
        return CompactRevisonException(self.revision)


class ServerCancelException(WatchException):
    def __init__(self, cancel_reason):
        super().__init__(f"Watch cancelled: {cancel_reason}")
        self.cancel_reason = cancel_reason
    
    def _clone(self):
        return ServerCancelException(self.cancel_reason)


class Watch(StubMixin):
    def __init__(self, channel, timeout):
        # Put (WatchCreateRequest, output_queue, done_future) to create a watch
        self._create_request_queue = _Pipe(5, loop=self._loop)
        # Put (output_queue, done_future) to cancel a watch
        self._cancel_request_queue = _Pipe(loop=self._loop)
        self._reconnect_event = asyncio.Event(loop=self._loop)
        self._watch_task_running = None
        super().__init__(channel, timeout)
    
    async def _watch_task(self, reconnect_event):
        # Queue for WatchRequest
        async def input_iterator(input_queue):
            while True:
                n = await input_queue.get()
                if n is None:
                    break
                yield n
        async def watch_call(input_queue, watch_stub, output_pipe):
            async with watch_stub.Watch.with_scope(input_iterator(input_queue)) as response_iter:
                async for r in response_iter:
                    await output_pipe.put(r)
        last_received_revision = None
        # watch_id -> revision
        last_watches_revision = {}
        # watch_id -> (WatchCreateRequest, output_queue)
        registered_watches = {}
        # output_queue -> watch_id
        registered_queues = {}
        # A tuple (WatchCreateRequest, output_queue, done_future, cancel_future)
        pending_create_request = None
        # watch_id -> done_future
        pending_cancel_requests = {}
        # output_queue -> (WatchCreateRequest, done_future)
        restore_creates = {}
        quitting = False
        def _reconnect_revision(watch_id):
            if last_received_revision is None:
                return None
            else:
                if watch_id in last_watches_revision:
                    last_revision = last_watches_revision[watch_id]
                    return max(last_revision + 1, last_received_revision)
                else:
                    return None
        try:
            while not quitting:         # Auto reconnect when failed or channel updated
                reconnect_event.clear()
                output_pipe = _Pipe(loop=self._loop)
                input_queue = asyncio.Queue(loop=self._loop)
                call_task = asyncio.ensure_future(watch_call(input_queue, self._watch_stub, output_pipe), loop=self._loop)
                try:
                    # Restore registered watches
                    for watch_id, (create_request, output_queue) in registered_watches.items():
                        if watch_id in pending_cancel_requests:
                            # Already cancelling
                            fut = pending_cancel_requests.pop(watch_id)
                            if not fut.done():
                                fut.set_result(True)
                            continue
                        r = rpc.WatchCreateRequest()
                        r.CopyFrom(create_request)
                        restore_revision = _reconnect_revision(watch_id)
                        if restore_revision is not None:
                            r.start_revision = restore_revision
                        restore_creates[output_queue] = (r, None)
                    registered_watches.clear()
                    registered_queues.clear()
                    # Restore pending cancels - should already be processed though
                    for watch_id, fut in pending_cancel_requests.items():
                        fut.set_result(True)
                    pending_cancel_requests.clear()
                    # Restore pending create request
                    if pending_create_request is not None:
                        if pending_create_request[3] is not None:     # Cancelled
                            pending_create_request[1].put_nowait((False, None, None))
                            if pending_create_request[2] is not None and not pending_create_request[2].done():
                                pending_create_request[2].set_result(True)
                            if pending_create_request[3] is not None and not pending_create_request[3].done():
                                pending_create_request[3].set_result(True)
                        else:
                            restore_creates[pending_create_request[1]] = (pending_create_request[0], pending_create_request[2])
                        pending_create_request = None
                    while True:
                        if pending_create_request is None:
                            if restore_creates:
                                q, (req, fut) = restore_creates.popitem()
                                # Send create request
                                pending_create_request = (req, q, fut, None)
                                input_queue.put_nowait(rpc.WatchRequest(create_request=req))
                        if pending_create_request is None:
                            select_pipes = [output_pipe, self._create_request_queue, self._cancel_request_queue]
                        else:
                            select_pipes = [output_pipe, self._cancel_request_queue]
                        reconn_wait = asyncio.ensure_future(reconnect_event.wait(), loop=self._loop)
                        select_futs = [reconn_wait, call_task]
                        if not pending_create_request and not registered_watches and \
                                not restore_creates:
                            select_futs.append(asyncio.sleep(2, loop=self._loop))
                        pipes, _ = await _select(select_pipes,
                                                    select_futs,
                                                    loop=self._loop)
                        reconn_wait.cancel()
                        if not pipes and not reconnect_event.is_set() and not call_task.done():
                            # No watch, stop the task
                            quitting = True
                            break
                        # Process cancel requests first
                        if self._cancel_request_queue in pipes:
                            cancel_requests = self._cancel_request_queue.read_nowait()
                            for output_queue, done_fut in cancel_requests:
                                if output_queue in pending_cancel_requests:
                                    # Chain this future
                                    pending_cancel_requests[output_queue].add_done_callback(
                                        lambda f, done_fut=done_fut: done_fut.set_result(True)
                                    )
                                elif output_queue in restore_creates:
                                    # Cancel a request which is not started
                                    _, fut = restore_creates.pop(output_queue)
                                    output_queue.put_nowait((False, None, None))
                                    if fut is not None and not fut.done():
                                        fut.set_result(True)
                                    if done_fut is not None and not done_fut.done():
                                        done_fut.set_result(True)
                                elif pending_create_request is not None and \
                                        pending_create_request[1] == output_queue:
                                    # Cancel the pending create watch
                                    if pending_create_request[3] is None:
                                        pending_create_request = pending_create_request[:3] + (done_fut,)
                                    else:
                                        pending_create_request[3].add_done_callback(
                                            lambda f, done_fut=done_fut: done_fut.set_result(True))
                                else:
                                    watch_id = registered_queues.get(output_queue)
                                    if watch_id is None:
                                        done_fut.set_result(True)
                                    else:
                                        # Send cancel request and save it to pending requests
                                        input_queue.put_nowait(
                                                rpc.WatchRequest(
                                                    cancel_request=
                                                        rpc.WatchCancelRequest(watch_id=watch_id)
                                                    )
                                                )
                                        pending_cancel_requests[watch_id] = done_fut
                        # Process received events
                        if output_pipe in pipes:
                            outputs = output_pipe.read_nowait()
                            for response in outputs:
                                if response.created:
                                    assert pending_create_request is not None
                                    if response.compact_revision > 0:
                                        # Cancelled (Is it possible?)
                                        exc = CompactRevisonException(response.compact_revision)
                                        pending_create_request[1].put_nowait((False, exc, response.compact_revision))
                                        if pending_create_request[2] is not None and not \
                                                pending_create_request[2].done():
                                            pending_create_request[2].set_exception(exc)
                                        if pending_create_request[3] is not None and not \
                                                pending_create_request[3].done():
                                            pending_create_request[3].set_result(True)
                                    else:
                                        registered_watches[response.watch_id] = pending_create_request[0:2]
                                        registered_queues[pending_create_request[1]] = response.watch_id
                                        if pending_create_request[2] is not None and not \
                                                pending_create_request[2].done():
                                            pending_create_request[2].set_result(True)
                                        if pending_create_request[3] is not None:
                                            # Immediately cancel the watch
                                            input_queue.put_nowait(
                                                    rpc.WatchRequest(
                                                        cancel_request=
                                                            rpc.WatchCancelRequest(watch_id=response.watch_id)
                                                        )
                                                    )
                                            pending_cancel_requests[response.watch_id] = pending_create_request[3]
                                    pending_create_request = None
                                if response.events:
                                    last_received_revision = response.header.revision
                                    last_watches_revision[response.watch_id] = last_received_revision
                                    if response.watch_id in registered_watches:
                                        _, output_queue = registered_watches[response.watch_id]
                                        output_queue.put_nowait((True,
                                                                 [Event(e, last_received_revision) for e in response.events],
                                                                 last_received_revision))
                                if response.compact_revision > 0:
                                    if response.watch_id in registered_watches:
                                        _, output_queue = registered_watches.pop(response.watch_id)
                                        exc = CompactRevisonException(response.compact_revision)
                                        output_queue.put_nowait((False, exc, response.compact_revision))
                                        del registered_queues[output_queue]
                                    if response.watch_id in pending_cancel_requests:
                                        if not pending_cancel_requests[response.watch_id].done():
                                            pending_cancel_requests[response.watch_id].set_result(True)
                                        del pending_cancel_requests[response.watch_id]
                                if response.canceled:
                                    # Cancel response
                                    if response.watch_id in registered_watches:
                                        _, output_queue = registered_watches.pop(response.watch_id)
                                        if response.watch_id in pending_cancel_requests:
                                            # Normal cancel
                                            output_queue.put_nowait((False, None, None))
                                        else:
                                            output_queue.put_nowait((False, ServerCancelException(response.cancel_reason), _reconnect_revision(response.watch_id)))
                                        del registered_queues[output_queue]
                                    if response.watch_id in pending_cancel_requests:
                                        if not pending_cancel_requests[response.watch_id].done():
                                            pending_cancel_requests[response.watch_id].set_result(True)
                                        del pending_cancel_requests[response.watch_id]
                        if self._create_request_queue in pipes:
                            while pending_create_request is None and not self._create_request_queue.is_empty():
                                create_req, output_queue, done_fut = self._create_request_queue.get_nowait()
                                if done_fut.done():
                                    # Ignore cancelled create requests
                                    output_queue.put_nowait((False, None, None))
                                    continue
                                # Send create request
                                pending_create_request = (create_req, output_queue, done_fut, None)
                                input_queue.put_nowait(rpc.WatchRequest(create_request=create_req))
                        if reconnect_event.is_set():
                            # Reconnected
                            break
                        if call_task.done():
                            # Maybe not available
                            if call_task.exception() is not None:
                                await call_task
                            else:
                                break
                finally:
                    input_queue.put_nowait(None)
                    call_task.cancel()
                    if quitting:
                        self._watch_task_running = None
                    try:
                        await call_task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass
        except Exception as exc:
            if registered_queues:
                for q, watch_id in registered_queues.items():
                    
                    q.put_nowait((False, exc, _reconnect_revision(watch_id)))
            if pending_create_request is not None:
                pending_create_request[1].put_nowait((False, exc, None))
                if pending_create_request[2] is not None and not pending_create_request[2].done():
                    pending_create_request[2].set_exception(exc)
                if pending_create_request[3] is not None and not pending_create_request[3].done():
                    pending_create_request[3].set_result(True)
            if pending_cancel_requests:
                for _, fut in pending_cancel_requests.items():
                    if not fut.done():
                        fut.set_result(True)
            if restore_creates:
                for q, (_, fut) in restore_creates.items():
                    if fut is not None and not fut.done():
                        fut.set_result(exc)
                    q.put_nowait((False, exc, _reconnect_revision(watch_id)))
            if not self._create_request_queue.is_empty():
                create_requests = self._create_request_queue.read_nowait()
                for r in create_requests:
                    r[1].put_nowait((False, exc, None))
                    if r[2] is not None and not r[2].done():
                        r[2].set_exception(exc)
            if not self._cancel_request_queue.is_empty():
                cancel_requests = self._cancel_request_queue.read_nowait()
                for _, fut in cancel_requests:
                    if fut is not None and not fut.done():
                        fut.set_result(True)

            if exc is CancelledError:
                raise
        except asyncio.CancelledError:
            raise

        finally:
            self._watch_task_running = None
    
    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._watch_stub = stub.WatchStub(channel)
        self._reconnect_event.set()

    def _ensure_watch_task(self):
        if self._watch_task_running is None:
            self._watch_task_running = asyncio.ensure_future(self._watch_task(self._reconnect_event))
    
    async def watch(self, key_range, start_revision=None, noput=False, nodelete=False, prev_kv=False,
                    always_reconnect=False, ignore_compact=False, batch_events=False, create_event=False):

        filters = []
        if noput:
            filters.append(rpc.WatchCreateRequest.NOPUT)
        if nodelete:
            filters.append(rpc.WatchCreateRequest.NODELETE)
        reconnect_revision = start_revision
        done_future = None
        try:
            while True:
                watch_request = rpc.WatchCreateRequest(start_revision=reconnect_revision,
                                                       filters=filters,
                                                       prev_kv=prev_kv)
        
                put_key_range(watch_request, key_range)
                self._ensure_watch_task()
                output_queue = asyncio.Queue(loop=self._loop)
                done_future = self._loop.create_future()
                await self._create_request_queue.put((watch_request, output_queue, done_future))
                try:
                    await done_future
                    if create_event:
                        yield None
                        create_event = False
                    while True:
                        is_event, result, revision = await output_queue.get()
                        if not is_event:
                            if revision is not None:
                                reconnect_revision = revision
                            if result is None:
                                break
                            else:
                                # When an exception is raised in multiple positions
                                # the traceback will mix up, so clone the exception
                                # for each raise
                                if isinstance(result, WatchException):
                                    raise result._clone() from result
                                else:
                                    raise WatchException("Watch failed with server exception") from result
                        else:
                            reconnect_revision = revision + 1
                            if batch_events:
                                yield tuple(result)
                            else:
                                for e in result:
                                    yield e
                except CompactRevisonException:
                    if ignore_compact:
                        continue
                    else:
                        raise
                except CancelledError:
                    raise
                except Exception:
                    if always_reconnect:
                        continue
                    else:
                        raise
                else:
                    break
        finally:
            if done_future is not None and not done_future.done():
                done_future.cancel()
            if self._watch_task_running:
                done_future = self._loop.create_future()
                await self._cancel_request_queue.put((output_queue, done_future))
                if self._watch_task_running:
                    try:
                        await done_future
                    except Exception:
                        pass

    def watch_scope(self, key_range, start_revision=None, noput=False, nodelete=False, prev_kv=False,
                    always_reconnect=False, ignore_compact=False, batch_events=False):
        return WatchScope(self.watch(key_range, start_revision=start_revision,
                                     noput=noput, nodelete=nodelete, prev_kv=prev_kv, create_event=True,
                                     always_reconnect=always_reconnect, ignore_compact=ignore_compact,
                                     batch_events=batch_events))
