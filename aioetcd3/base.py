import asyncio
from grpc import (
    metadata_call_credentials, AuthMetadataPlugin, RpcError, StatusCode
)

from .exceptions import AuthError, STATUS_MAP


_default_timeout = object()


class _EtcdTokenCallCredentials(AuthMetadataPlugin):

    def __init__(self, access_token):
        self._access_token = access_token

    def __call__(self, context, callback):
        metadata = (("token", self._access_token),)
        callback(metadata, None)


class StubMixin(object):
    def __init__(self, channel, timeout, username=None, password=None):
        self.username = username
        self.password = password
        self.channel = channel
        self.timeout = timeout
        self._auth_lock = asyncio.Lock()
        self.last_response_info = None
        self._metadata = None
        self._call_credentials = None
        self._update_channel(channel)

    async def _authenticate(self):
        async with self._auth_lock:  # Avoiding concurrent authentications for the client instance
            if self._metadata is not None:  # Avoiding double authentication
                return
            token = await self.authenticate(username=self.username, password=self.password)
            self._metadata = (("token", token),)
            self._call_credentials = metadata_call_credentials(_EtcdTokenCallCredentials(token))

    def _update_channel(self, channel):
        self.channel = channel
        self._loop = channel._loop

    def _update_cluster_info(self, header):
        self.last_response_info = header

    def get_cluster_info(self):
        return self.last_response_info

    async def grpc_call(self, stub_func, request, timeout=_default_timeout, skip_auth=False):
        if timeout is _default_timeout:
            timeout = self.timeout

        # If the username and password are set, trying to call the auth.authenticate
        # method to get the auth token. If the token already received - just use it.
        if self.username is not None and self.password is not None and not skip_auth:
            if self._metadata is None:  # We need to call self._authenticate for the first rpc call only
                try:
                    await self._authenticate()
                except RpcError as exc:
                    if exc._state.code == StatusCode.INVALID_ARGUMENT:
                        raise AuthError(exc._state.details, exc._state.debug_error_string)
                    raise exc

        try:
            response = await stub_func(
                request, timeout=timeout, credentials=self._call_credentials, metadata=self._metadata
            )
        except RpcError as exc:
            _process_rpc_error(exc)
        self._update_cluster_info(response.header)
        return response


def _process_rpc_error(exc: RpcError):
    """Wraps grpc.RpcError to a specific library's exception.
    If there is no specific exception found in the map, the original
    exception will be raised
    """
    try:
        new_exc = STATUS_MAP.get(exc._state.code)
        if new_exc is not None:
            raise new_exc(exc._state.details, exc._state.debug_error_string)
    except AttributeError:
        pass
    raise exc
