class StubMixin(object):
    def __init__(self, channel, timeout=5):
        self.channel = channel
        self.timeout = timeout
        self.last_response_info = None
        self._update_channel(channel)

    def _update_channel(self, channel):
        pass

    def _update_cluster_info(self, header):
        self.last_response_info = header

    def get_cluster_info(self):
        return self.last_response_info

    async def grpc_call(self, stub_func, request, timeout=5):
        response = await stub_func(request, timeout=timeout)
        self._update_cluster_info(response.header)
        return response
