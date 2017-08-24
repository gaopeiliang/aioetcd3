# aioetcd3
[![Build Status](https://travis-ci.org/gaopeiliang/aioetcd3.svg?branch=master)](https://travis-ci.org/gaopeiliang/aioetcd3)
[![Code Coverage](https://codecov.io/gh/gaopeiliang/aioetcd3/branch/master/graphs/badge.svg)](https://codecov.io/gh/gaopeiliang/aioetcd3)

### AsyncIO bindings for etcd V3

example:
``` 
from aioetcd3.client import client
from aioetcd3.help import range_all
from aioetcd3.kv import KV
from aioetcd3 import transaction

etcd_client = client(endpoints="127.0.0.1:2379")

    await etcd_client.put('/foo', 'foo')
    
    value, meta = await etcd_client.get('/foo')
    
    value_list = await etcd_client.range(range_all())
    
    await etcd_client.delete('/foo')
    
    
    lease = await etcd_client.grant_lease(ttl=5)
    
    await etcd_client.put('/foo1', 'foo', lease=lease)
    
    is_success, response = await etcd_client.txn(compare=[
            transaction.Value('/trans1') == b'trans1',
            transaction.Value('/trans2') == b'trans2'
        ], success=[
            KV.delete.txn('/trans1'),
            KV.put.txn('/trans3', 'trans3', prev_kv=True)
        ], fail=[
            KV.delete.txn('/trans1')
        ])
      
    await self.client.user_add(username="test user", password='1234')
    await self.client.role_add(name="test_role")
```


