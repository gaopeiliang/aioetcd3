from aioetcd3._etcdv3 import rpc_pb2 as rpc
from aioetcd3.utils import to_bytes, put_key_range
from aioetcd3.base import StubMixin, _default_timeout
from inspect import getcallargs
import functools
import aioetcd3._etcdv3.rpc_pb2_grpc as stub


class KVMetadata(object):
    def __init__(self, keyvalue):
        # self.key = keyvalue.key
        self.create_revision = keyvalue.create_revision
        self.mod_revision = keyvalue.mod_revision
        self.version = keyvalue.version
        self.lease_id = keyvalue.lease

_default = object()


_sort_order_dict = {"ascend": rpc.RangeRequest.ASCEND,
                    None: rpc.RangeRequest.NONE,
                    "descend": rpc.RangeRequest.DESCEND}

_sort_target_dict = {"key": rpc.RangeRequest.KEY,
                     None: rpc.RangeRequest.KEY,
                     'version': rpc.RangeRequest.VERSION,
                     'create': rpc.RangeRequest.CREATE,
                     'mod': rpc.RangeRequest.MOD,
                     'value': rpc.RangeRequest.VALUE}


def _get_grpc_args(func, *args, **kwargs):
    params = getcallargs(func, None, *args, **kwargs)
    params.pop('self')
    params.pop('timeout')
    return params


def _kv(request_builder, response_builder, method):
    def _decorator(f):
        def txn(*args, timeout=_default_timeout, **kwargs):
            call_args = _get_grpc_args(f, *args, **kwargs)
            return (request_builder(**call_args), response_builder(**call_args))
        f.txn = txn

        @functools.wraps(f)
        async def grpc_func(self, *args, timeout=_default_timeout, **kwargs):
            request, response = txn(*args, **kwargs)
            return response(await self.grpc_call(method(self), request, timeout=timeout))
        return grpc_func
    return _decorator


def _create_txn_response_builder(success, fail, **kwargs):
    def _response_builder(response):
        if response.succeeded:
            return True, [t[1](_get_op_response(r)) for t, r in zip(success, response.responses)]
        else:
            return False, [t[1](_get_op_response(r)) for t, r in zip(fail, response.responses)]
    return _response_builder



def _range_request(key_range, sort_order=None, sort_target='key', **kwargs):
    range_request = rpc.RangeRequest()
    put_key_range(range_request, key_range)

    for k, v in kwargs.items():
        if v is not None:
            setattr(range_request, k, v)

    if sort_order in _sort_order_dict:
        range_request.sort_order = _sort_order_dict[sort_order]
    else:
        raise ValueError('unknown sort order: "{}"'.format(sort_order))

    if sort_target in _sort_target_dict:
        range_request.sort_target=_sort_target_dict[sort_target]
    else:
        raise ValueError('sort_target must be one of "key", '
                         '"version", "create", "mod" or "value"')

    return range_request


def _range_response(kv_response):
    result = []
    for kv in kv_response.kvs:
        result.append((kv.key, kv.value, KVMetadata(kv)))
    return result


def _static_builder(f):
    def _builder(*args, **kwargs):
        return f
    return _builder


def _partial_builder(f):
    def _builder(**kwargs):
        return functools.partial(f, **kwargs)
    return _builder


def _put_request(key, value, lease=None, prev_kv=False, ignore_value=False, ignore_lease=False):
    if lease is None:
        lease = 0
    elif hasattr(lease, 'id'):
        lease = lease.id
    put_request = rpc.PutRequest(key=to_bytes(key),
                                 prev_kv=prev_kv, ignore_value=ignore_value,
                                 ignore_lease=ignore_lease)

    if not ignore_value:
        put_request.value = to_bytes(value)

    if not ignore_lease:
        put_request.lease = lease

    return put_request


def _delete_request(key_range, prev_kv=False):

    delete_request = rpc.DeleteRangeRequest(prev_kv=prev_kv)
    put_key_range(delete_request, key_range)

    return delete_request


def _get_response(response):
    if response.kvs:
        return response.kvs[0].value, KVMetadata(response.kvs[0])
    else:
        return None, None


def _range_keys_response(response):
    result = []
    for kv in response.kvs:
        result.append((kv.key, KVMetadata(kv)))

    return result


def _delete_response(response, prev_kv=False, **kwargs):
    # when set prev_kv to return prev value,
    # but it is not existed , response has no prev_kvs
    if prev_kv:
        r = []
        for kv in response.prev_kvs:
            r.append((kv.key, kv.value, KVMetadata(kv)))
        return r
    else:
        return response.deleted


def _put_response(response, prev_kv=False, **kwargs):

    # when set prev_kv to return prev value,
    # but it is not existed , response has no prev_kv
    if prev_kv and response.HasField('prev_kv'):
        return response.prev_kv.value, KVMetadata(response.prev_kv)
    else:
        return None, None


def _create_op_request(request):
    if isinstance(request, rpc.PutRequest):
        return rpc.RequestOp(request_put=request)
    elif isinstance(request, rpc.RangeRequest):
        return rpc.RequestOp(request_range=request)
    elif isinstance(request, rpc.DeleteRangeRequest):
        return rpc.RequestOp(request_delete_range=request)
    elif isinstance(request, rpc.TxnRequest):
        return rpc.RequestOp(request_txn=request)
    else:
        raise TypeError("Unsupported request OP: " + repr(request))


def _get_op_response(response):
    return getattr(response, response.WhichOneof('response'))


def _compare_request(compare, success, fail):
    compare_message = [c.build_message() for c in compare]
    success_message = [_create_op_request(request=r) for r, _ in success]
    fail_message = [_create_op_request(request=r) for r, _ in fail]
    request = rpc.TxnRequest(compare=compare_message, success=success_message, failure=fail_message)
    return request


class KV(StubMixin):
    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._kv_stub = stub.KVStub(channel)

    @_kv(_range_request, _static_builder(_range_response), lambda x: x._kv_stub.Range)
    async def range(self, key_range, limit=None, revision=None, timeout=_default_timeout, sort_order=None, sort_target='key',
                    serializable=None, keys_only=None, count_only=None, min_mod_revision=None, max_mod_revision=None,
                    min_create_revision=None, max_create_revision=None):
        # implemented in decorator
        pass

    @_kv(functools.partial(_range_request, count_only=True),
         _static_builder(lambda r: r.count), lambda x: x._kv_stub.Range)
    async def count(self, key_range, revision=None, timeout=_default_timeout, min_mod_revision=None,
                    max_mod_revision=None, min_create_revision=None, max_create_revision=None):
        pass

    @_kv(functools.partial(_range_request, keys_only=True), _static_builder(_range_keys_response),
         lambda x: x._kv_stub.Range)
    async def range_keys(self, key_range, limit=None, revison=None, sort_order=None,
                         sort_target='key', timeout=_default_timeout, serializable=None, count_only=None,
                         min_mod_revision=None, max_mod_revision=None, min_create_revision=None,
                         max_create_revision=None):
        pass

    @_kv(_range_request, _static_builder(_get_response), lambda x: x._kv_stub.Range)
    async def get(self, key_range, revision=None, timeout=_default_timeout, serializable=None,
                  min_mod_revision=None, max_mod_revision=None, min_create_revision=None,
                  max_create_revision=None):
        pass

    @_kv(_put_request, _partial_builder(_put_response), lambda x: x._kv_stub.Put)
    async def put(self, key, value, lease=0, prev_kv=False, timeout=_default_timeout, ignore_value=False, ignore_lease=False):
        pass

    @_kv(_delete_request, _partial_builder(_delete_response), lambda x: x._kv_stub.DeleteRange)
    async def delete(self, key_range, timeout=_default_timeout, prev_kv=False):
        pass

    @_kv(functools.partial(_delete_request, prev_kv=True),
         _partial_builder(functools.partial(_delete_response, prev_kv=True)),
         lambda x: x._kv_stub.DeleteRange)
    async def pop(self, key_range, timeout=_default_timeout):
        pass

    @_kv(_compare_request, _create_txn_response_builder, lambda x: x._kv_stub.Txn)
    async def txn(self, compare, success, fail=[], *, timeout=_default_timeout):
        pass

    async def compact(self, revision, physical=False, *, timeout=_default_timeout):
        """
        Compact etcd KV storage
        
        :param revision: compact to specified revision
        
        :param physical: return until data is physically compacted
        
        :param timeout: maximum time to wait
        """
        await self.grpc_call(self._kv_stub.Compact,
                               rpc.CompactionRequest(revision=revision,
                                                    physical=physical),
                               timeout=timeout)
        