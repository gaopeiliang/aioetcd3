import functools

from aioetcd3.base import StubMixin
from aioetcd3._etcdv3 import rpc_pb2 as rpc
from aioetcd3._etcdv3 import auth_pb2 as auth
from aioetcd3.utils import put_key_range
import aioetcd3._etcdv3.rpc_pb2_grpc as stub


def call_grpc(request, response_func, method):

    def _f(f):
        @functools.wraps(f)
        async def call(self, *args, **kwargs):
            r = await self.grpc_call(method(self), request(*args, **kwargs))
            return response_func(r)

        return call

    return _f


class Auth(StubMixin):
    def _update_channel(self, channel):
        super()._update_channel(channel)
        self._auth_stub = stub.AuthStub(channel)

    @call_grpc(lambda: rpc.AuthEnableRequest(), lambda r: None, lambda s: s._auth_stub.AuthEnable)
    async def auth_enable(self):
        pass

    @call_grpc(lambda: rpc.AuthDisableRequest(), lambda r: None, lambda s: s._auth_stub.AuthDisable)
    async def auth_disable(self):
        pass

    @call_grpc(lambda username, password: rpc.AuthenticateRequest(name=username, password=password),
               lambda r: r.token, lambda s: s._auth_stub.Authenticate)
    async def authenticate(self, username, password):
        pass

    @call_grpc(lambda: rpc.AuthUserListRequest(), lambda r: [u for u in r.users], lambda s: s._auth_stub.UserList)
    async def user_list(self):
        pass

    @call_grpc(lambda username: rpc.AuthUserGetRequest(name=username), lambda r: [r for r in r.roles],
               lambda s: s._auth_stub.UserGet)
    async def user_get(self, username):
        pass

    @call_grpc(lambda username, password: rpc.AuthUserAddRequest(name=username, password=password), lambda r: None,
               lambda s: s._auth_stub.UserAdd)
    async def user_add(self, username, password):
        pass

    @call_grpc(lambda username: rpc.AuthUserDeleteRequest(name=username), lambda r: None,
               lambda s: s._auth_stub.UserDelete)
    async def user_delete(self, username):
        pass

    @call_grpc(lambda username, password: rpc.AuthUserChangePasswordRequest(name=username, password=password),
               lambda r: None, lambda s: s._auth_stub.UserChangePassword)
    async def user_change_password(self, username, password):
        pass

    @call_grpc(lambda username, role: rpc.AuthUserGrantRoleRequest(user=username, role=role), lambda r: None,
               lambda s: s._auth_stub.UserGrantRole)
    async def user_grant_role(self, username, role):
        pass

    @call_grpc(lambda username, role: rpc.AuthUserRevokeRoleRequest(name=username, role=role), lambda r: None,
               lambda s: s._auth_stub.UserRevokeRole)
    async def user_revoke_role(self, username, role):
        pass

    @call_grpc(lambda: rpc.AuthRoleListRequest(), lambda r: [role for role in r.roles],
               lambda s: s._auth_stub.RoleList)
    async def role_list(self):
        pass

    @call_grpc(lambda name: rpc.AuthRoleGetRequest(role=name), lambda r: [p for p in r.perm],
               lambda s: s._auth_stub.RoleGet)
    async def role_get(self, name):
        pass

    @call_grpc(lambda name: rpc.AuthRoleAddRequest(name=name), lambda r: None, lambda s: s._auth_stub.RoleAdd)
    async def role_add(self, name):
        pass

    @call_grpc(lambda name: rpc.AuthRoleDeleteRequest(role=name), lambda r: None, lambda s: s._auth_stub.RoleDelete)
    async def role_delete(self, name):
        pass

    @staticmethod
    def role_grant_request(name, key_range, permission):
        if permission not in [auth.Permission.READ, auth.Permission.WRITE, auth.Permission.READWRITE]:
            raise ValueError("permission must be read, write or readwrite")
        per = auth.Permission(permType=permission)
        put_key_range(per, key_range)

        request = rpc.AuthRoleGrantPermissionRequest(name=name, perm=per)

        return request

    @call_grpc(role_grant_request.__func__, lambda r: None, lambda s: s._auth_stub.RoleGrantPermission)
    async def role_grant_permission(self, name, key_range, permission):
        pass

    @staticmethod
    def role_revoke_request(name, key_range):
        request = rpc.AuthRoleRevokePermissionRequest(role=name)
        put_key_range(request, key_range)

        return request

    @call_grpc(role_revoke_request.__func__, lambda r: None, lambda s: s._auth_stub.RoleRevokePermission)
    async def role_revoke_permission(self, name, key_range):
        pass
