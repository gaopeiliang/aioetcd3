_default_timeout = object()

class StubMixin(object):
    def __init__(self, channel, timeout):
        self.channel = channel
        self.timeout = timeout
        self.last_response_info = None
        self._update_channel(channel)

    def _update_channel(self, channel):
        self.channel = channel
        self._loop = channel._loop

    def _update_cluster_info(self, header):
        self.last_response_info = header

    def get_cluster_info(self):
        return self.last_response_info

    async def grpc_call(self, stub_func, request, timeout=_default_timeout):
        if timeout is _default_timeout:
            timeout = self.timeout
        response = await stub_func(request, timeout=timeout)
        self._update_cluster_info(response.header)
        return response
