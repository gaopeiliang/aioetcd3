import unittest
import asyncio
import functools

from aioetcd3.client import client, ssl_client, set_grpc_cipher
from aioetcd3.help import range_all, PER_RW
from aioetcd3.exceptions import AuthError, Unauthenticated, PermissionDenied

from .utils import switch_auth_off, switch_auth_on


def asynctest(f):
    @functools.wraps(f)
    def _f(self):
        asyncio.get_event_loop().run_until_complete(f(self))

    return _f


TEST_USER_NAME = 'test'
TEST_USER_PASSWORD = "test"
TEST_ROLE_NAME = 'admin'


class AuthTest(unittest.TestCase):
    @asynctest
    async def setUp(self):
        endpoints = "127.0.0.1:2379"
        self.client = client(endpoint=endpoints)

        set_grpc_cipher()
        auth_etcd_url = "127.0.0.1:2378"
        self.root_client = ssl_client(endpoint=auth_etcd_url, ca_file="test/cfssl/ca.pem",
                                      cert_file="test/cfssl/client-root.pem",
                                      key_file="test/cfssl/client-root-key.pem")

        self.client_client = ssl_client(endpoint=auth_etcd_url, ca_file="test/cfssl/ca.pem",
                                        cert_file="test/cfssl/client.pem",
                                        key_file="test/cfssl/client-key.pem")

        await self.cleanUp()

    @asynctest
    async def test_auth_1(self):

        await self.client.user_add(username=TEST_USER_NAME, password='1234')
        users = await self.client.user_list()

        self.assertIn(TEST_USER_NAME, users)

        roles = await self.client.user_get(username=TEST_USER_NAME)
        self.assertEqual(len(roles), 0)

        await self.client.user_change_password(username=TEST_USER_NAME, password=TEST_USER_PASSWORD)

        await self.client.user_delete(username=TEST_USER_NAME)

    @asynctest
    async def test_auth_2(self):

        await self.client.role_add(name=TEST_ROLE_NAME)

        roles = await self.client.role_list()
        self.assertIn(TEST_ROLE_NAME, roles)

        role_info = await self.client.role_get(name=TEST_ROLE_NAME)

        await self.client.role_delete(name=TEST_ROLE_NAME)

    @asynctest
    async def test_auth_3(self):

        await self.client.user_add(username=TEST_USER_NAME, password=TEST_USER_PASSWORD)
        with self.assertRaises(Exception):
            await self.client.user_grant_role(username=TEST_USER_NAME, role=TEST_ROLE_NAME)

        await self.client.role_add(name=TEST_ROLE_NAME)
        await self.client.user_grant_role(username=TEST_USER_NAME, role=TEST_ROLE_NAME)

        await self.client.role_grant_permission(name=TEST_ROLE_NAME,
                                                key_range=range_all(),
                                                permission=PER_RW)

        await self.client.user_revoke_role(username=TEST_USER_NAME, role=TEST_ROLE_NAME)

        await self.client.role_revoke_permission(name=TEST_ROLE_NAME,
                                                 key_range=range_all())


    @asynctest
    async def test_auth_4(self):
        await self.root_client.user_add(username='root', password='root')
        await self.root_client.role_add(name='root')
        await self.root_client.user_grant_role(username='root', role='root')
        await self.root_client.auth_enable()

        await self.root_client.user_add(username='client', password='client')
        await self.root_client.role_add(name='client')

        await self.root_client.put('/foo', '/foo')
        value, meta = await self.root_client.get('/foo')
        self.assertEqual(value, b'/foo')

        with self.assertRaises(Exception):
            await self.client_client.get('/foo')

        await self.root_client.role_grant_permission(name='client', key_range='/foo', permission=PER_RW)
        await self.root_client.user_grant_role(username='client', role='client')

        value, meta = await self.client_client.get('/foo')
        self.assertEqual(value, b'/foo')

        await self.client_client.put('/foo', 'ssss')

    async def delete_all_user(self):
        users = await self.client.user_list()

        for u in users:
            await self.client.user_delete(username=u)

        users = await self.root_client.user_list()

        for u in users:
            await self.root_client.user_delete(username=u)

    async def delete_all_role(self):
        roles = await self.client.role_list()

        for r in roles:
            await self.client.role_delete(name=r)

        roles = await self.root_client.role_list()

        for r in roles:
            await self.root_client.role_delete(name=r)

    async def cleanUp(self):

        await self.client.delete(range_all())

        await self.root_client.auth_disable()

        await self.delete_all_user()
        await self.delete_all_role()
    
    @asynctest
    async def tearDown(self):
        await self.cleanUp()
        await self.client.close()


class PasswordAuthTest(unittest.TestCase):
    @asynctest
    async def setUp(self):
        self.endpoints = "127.0.0.1:2379"
        self.unauthenticated_client = client(endpoint=self.endpoints)
        await self.cleanUp()
        await switch_auth_on(self.unauthenticated_client)
        self.client_client = client(
            endpoint=self.endpoints, username="client", password="client"
        )
        self.root_client = client(endpoint=self.endpoints, username="root", password="root")

    async def create_kv_for_test(self):
        await self.root_client.put('/foo', '/foo')
        value, meta = await self.root_client.get('/foo')
        self.assertEqual(value, b'/foo')

    @asynctest
    async def test_auth_1(self):
        await self.create_kv_for_test()

        with self.assertRaises(PermissionDenied):
            await self.client_client.get('/foo')

        await self.root_client.role_grant_permission(name='client', key_range='/foo', permission=PER_RW)
        value, meta = await self.client_client.get('/foo')
        self.assertEqual(value, b'/foo')

        await self.client_client.put('/foo', 'ssss')

    @asynctest
    async def test_wrong_password(self):
        wrong_password_client = client(endpoint=self.endpoints, username="client", password="wrong_password")
        with self.assertRaises(AuthError) as exc:
            await wrong_password_client.get("/foo")
        assert repr(exc.exception) == "`{}`: reason: `{}`".format(exc.exception.code, exc.exception.details)

    @asynctest
    async def test_wrong_token(self):
        await self.create_kv_for_test()
        await self.root_client.role_grant_permission(name='client', key_range='/foo', permission=PER_RW)

        new_client = client(endpoint=self.endpoints, username="client", password="client")
        value, meta = await self.client_client.get('/foo')
        self.assertEqual(value, b'/foo')

        # Put invalid token
        new_client._metadata = (("token", "invalid_token"),)
        with self.assertRaises(Unauthenticated) as exc:
            await new_client.get("/foo")

    async def cleanUp(self):
        await self.unauthenticated_client.delete(range_all())

    @asynctest
    async def tearDown(self):
        await switch_auth_off(self.root_client, self.unauthenticated_client)
        await self.cleanUp()


class PasswordAuthWithSslTest(unittest.TestCase):
    @asynctest
    async def setUp(self):
        self.endpoints = "127.0.0.1:2377"
        self.unauthenticated_client = ssl_client(
            endpoint=self.endpoints,
            ca_file="test/cfssl/ca.pem",
        )
        await self.cleanUp()
        await switch_auth_on(self.unauthenticated_client)
        self.root_client = ssl_client(endpoint=self.endpoints, ca_file="test/cfssl/ca.pem",
                                      username="root", password="root")

        self.client_client = ssl_client(endpoint=self.endpoints, ca_file="test/cfssl/ca.pem",
                                        username="client", password="client")

    async def create_kv_for_test(self):
        await self.root_client.put('/foo', '/foo')
        value, meta = await self.root_client.get('/foo')
        self.assertEqual(value, b'/foo')

    @asynctest
    async def test_auth_1(self):
        await self.create_kv_for_test()

        with self.assertRaises(PermissionDenied):
            await self.client_client.get('/foo')

        await self.root_client.role_grant_permission(name='client', key_range='/foo', permission=PER_RW)
        value, meta = await self.client_client.get('/foo')
        self.assertEqual(value, b'/foo')

        await self.client_client.put('/foo', 'ssss')

    @asynctest
    async def test_wrong_password(self):
        wrong_password_client = ssl_client(
            endpoint=self.endpoints, ca_file="test/cfssl/ca.pem",
            username="client", password="wrong_password"
        )
        with self.assertRaises(AuthError) as exc:
            await wrong_password_client.get("/foo")
        assert repr(exc.exception) == "`{}`: reason: `{}`".format(exc.exception.code, exc.exception.details)

    @asynctest
    async def test_wrong_token(self):
        await self.create_kv_for_test()
        await self.root_client.role_grant_permission(name='client', key_range='/foo', permission=PER_RW)

        new_client = ssl_client(
            endpoint=self.endpoints, ca_file="test/cfssl/ca.pem",
            username="root", password="root"
        )
        value, meta = await new_client.get('/foo')
        self.assertEqual(value, b'/foo')

        # Put invalid token
        new_client._metadata = (("token", "invalid_token"),)
        with self.assertRaises(Unauthenticated) as exc:
            await new_client.get("/foo")

    async def cleanUp(self):
        await self.unauthenticated_client.delete(range_all())

    @asynctest
    async def tearDown(self):
        await switch_auth_off(self.root_client, self.unauthenticated_client)
        await self.cleanUp()
