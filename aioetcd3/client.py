import grpc
from aiogrpc.channel import Channel
from aioetcd3.kv import KV


class Client(object):
    def __init__(self, host='localhost', port=2379,
                 ca_cert=None, cert_key=None, cert_cert=None, timeout=None):
        self._url = '{host}:{port}'.format(host=host, port=port)

        cert_params = [c is not None for c in (cert_cert, cert_key, ca_cert)]
        if all(cert_params):
            # all the cert parameters are set
            credentials = self._get_secure_creds(ca_cert,
                                                 cert_key,
                                                 cert_cert)
            self.channel = grpc.secure_channel(self._url, credentials)
        elif any(cert_params):
            # some of the cert parameters are set
            raise ValueError('the parameters cert_cert, cert_key and ca_cert '
                             'must all be set to use a secure channel')
        else:
            self.channel = grpc.insecure_channel(self._url)

        # use aiogrpc to decorate channel
        self.channel = Channel(self.channel)

        self.timeout = timeout
        self.kv = KV(self, self.channel, self.timeout)

    @staticmethod
    def _get_secure_creds(ca_cert, cert_key, cert_cert):
        with open(ca_cert, 'rb') as ca_cert_file:
            with open(cert_key, 'rb') as cert_key_file:
                with open(cert_cert, 'rb') as cert_cert_file:
                    return grpc.ssl_channel_credentials(
                        ca_cert_file.read(),
                        cert_key_file.read(),
                        cert_cert_file.read()
                    )


def client(host='localhost', port=2379,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None):
    """Return an instance of an AioEtcd3Client."""
    return Client(host=host,
                  port=port, ca_cert=ca_cert, cert_key=cert_key,
                  cert_cert=cert_cert, timeout=timeout)



