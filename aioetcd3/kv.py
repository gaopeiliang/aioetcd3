from aioetcd3.rpc import rpc_pb2 as rpc
from aioetcd3.utils import to_bytes, increment_last_byte


class KVMetadata(object):
    def __init__(self, keyvalue):
        self.key = keyvalue.key
        self.create_revision = keyvalue.create_revision
        self.mod_revision = keyvalue.mod_revision
        self.version = keyvalue.version
        self.lease_id = keyvalue.lease


class KV(object):
    def __init__(self, client, channel, timeout=None):
        self.stub = rpc.KVStub(channel)
        self.timeout = timeout

    @staticmethod
    def build_range_request(key,
                            range_end=None, limit=None, revision=None, sort_order=None, sort_target='key',
                            serializable=False, keys_only=False, count_only=False, min_mod_revision=None,
                            max_mod_revision=None, min_create_revision=None, max_create_revision=None):
        range_request = rpc.RangeRequest()
        range_request.key = to_bytes(key)

        if range_end is not None:
            range_request.range_end = to_bytes(range_end)
        if limit is not None:
            range_request.limit = int(limit)
        if revision is not None:
            range_request.revision = int(revision)
        if min_mod_revision is not None:
            range_request.min_mod_revision = int(min_mod_revision)
        if max_mod_revision is not None:
            range_request.max_mod_revision = int(max_mod_revision)
        if min_create_revision is not None:
            range_request.min_create_revision = int(min_create_revision)
        if max_create_revision is not None:
            range_request.max_create_revision = int(max_create_revision)

        if serializable:
            range_request.serializable = True
        if keys_only:
            range_request.keys_only = True
        if count_only:
            range_request.count_only = True

        sort_order_dict = {"ascend": rpc.RangeRequest.ASCEND,
                           None: rpc.RangeRequest.NONE,
                           "descend": rpc.RangeRequest.DESCEND}

        if sort_order in sort_order_dict:
            range_request.sort_order = sort_order_dict[sort_order]
        else:
            raise ValueError('unknown sort order: "{}"'.format(sort_order))

        sort_target_dict = {"key": rpc.RangeRequest.KEY,
                            None: rpc.RangeRequest.KEY,
                            'version': rpc.RangeRequest.VERSION,
                            'create': rpc.RangeRequest.CREATE,
                            'mod': rpc.RangeRequest.MOD,
                            'value': rpc.RangeRequest.VALUE}

        if sort_target in sort_target_dict:
            range_request.sort_target=sort_target_dict[sort_target]
        else:
            raise ValueError('sort_target must be one of "key", '
                             '"version", "create", "mod" or "value"')

        return range_request

    @staticmethod
    def build_put_request(key, value, lease=0, pre_kv=False, ignore_value=False, ignore_lease=False):

        put_request = rpc.PutRequest()
        put_request.key = to_bytes(key)
        put_request.value = to_bytes(value)
        put_request.lease = int(lease)
        put_request.prev_kv = pre_kv
        put_request.ignore_value = ignore_value
        put_request.ignore_lease = ignore_lease

        return put_request

    @staticmethod
    def build_delete_request(key, range_end=None, prev_kv=False):
        delete_request = rpc.DeleteRangeRequest()
        delete_request.key = to_bytes(key)
        delete_request.range_end = to_bytes(range_end)
        delete_request.prev_kv = prev_kv

        return delete_request

    async def get(self, key, prefix=False, greater=False, range_end=None, revision=None,
                  limit=None, sort_order=None, sort_target='key'):
        """
        Get retrieves keys.
        :param key: get key name
        :param prefix: true -> get with key as prefix
        :param greater: true -> get from key until all
        :param range_end: range the last key
        :param revision: with revision > 0 , get the revision space key value
        :param limit: the max number item get
        :param sort_order: sort strategy
        :param sort_target: sort the return value with field
        :return: iter all get (value, meta)
        """
        params_dict = dict()
        if prefix:
            range_end = increment_last_byte(to_bytes(key))
        elif greater:
            range_end = b'\0'

        if range_end:
            params_dict.update({"range_end": range_end})

        if revision:
            params_dict.update({"revision": revision})
        if limit:
            params_dict.update({"limit": limit})
        if sort_order:
            params_dict.update({"sort_order": sort_order})
        if sort_target:
            params_dict.update({"sort_target": sort_target})

        request = self.build_range_request(key=key, **params_dict)
        response = await self.stub.Range(request, self.timeout)

        for kv in response.kvs:
            yield kv.value, KVMetadata(kv)

    async def get_all(self, revision=None, limit=None, sort_order=None, sort_target="key"):
        """
        get all key value
        :param revision:  with revision > 0 , get the revision space key value
        :param limit: the max number item get
        :param sort_order: sort strategy
        :param sort_target: sort the return value with field
        :return: iter all get (value, meta)
        """
        params_dict = {"key": b'\0', "range_end": b'\0'}

        if revision:
            params_dict.update({"revision": revision})
        if limit:
            params_dict.update({"limit": limit})
        if sort_order:
            params_dict.update({"sort_order": sort_order})
        if sort_target:
            params_dict.update({"sort_target": sort_target})

        request = self.build_range_request(**params_dict)
        response = await self.stub.Range(request, self.timeout)

        for kv in response.kvs:
            yield kv.value, KVMetadata(kv)

    async def put(self, key, value, lease=0, prev_kv=False, ignore_value=False, ignore_lease=False):
        """
        put key value to store
        :param key: ...
        :param value: ...
        :param lease: this kv pair attached lease id
        :param prev_kv: return the last value
        :param ignore_value:
        :param ignore_lease:
        :return:
        """

        request = self.build_put_request(key=key, value=value, pre_kv=prev_kv,
                                         ignore_value=ignore_value, ignore_lease=ignore_lease)

        response = await self.stub.Put(request, self.timeout)

        if prev_kv:
            return response.prev_kv, KVMetadata(response.prev_kv)
        else:
            return None, None

    async def delete(self, key, prefix=False, greater=False, range_end=None, prev_kv=False):

        params_dict = dict()
        if prefix:
            range_end = increment_last_byte(to_bytes(key))
        elif greater:
            range_end = b'\0'

        if range_end:
            params_dict.update({"range_end": range_end})
        if prev_kv:
            params_dict.update({"prev_kv": prev_kv})

        request = self.build_delete_request(key=key, **params_dict)

        response = await self.stub.DeleteRange(request, self.timeout)

        if prev_kv:
            for kv in response.prev_kvs:
                yield kv.value, KVMetadata(kv)
        else:
            return



