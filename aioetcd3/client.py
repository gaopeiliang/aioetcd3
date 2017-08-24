import os
import grpc
from aiogrpc.channel import Channel
from aioetcd3.kv import KV
from aioetcd3.lease import Lease
from aioetcd3.auth import Auth


class Client(KV, Lease, Auth):
    def __init__(self, endpoints,
                 ca_cert=None, cert_key=None, cert_cert=None, timeout=None):
        self.channel, self.credentials = self.create_grpc_channel(endpoints=endpoints,
                                                                  ca_cert=ca_cert,
                                                                  cert_key=cert_key, cert_cert=cert_cert)
        self.timeout = timeout
        super().__init__(self.channel, self.timeout)
    #
    # def update_server_list(self, ...):
    #     ...
    #     self.channel = ...
    #     self._update_channel(self.channel)

    def create_grpc_channel(self, endpoints, ca_cert=None, cert_key=None, cert_cert=None):
        cert_params = [c is not None for c in (cert_cert, cert_key, ca_cert)]
        credentials = None
        if all(cert_params):
            # all the cert parameters are set
            credentials = self._get_secure_creds(ca_cert,
                                                 cert_key,
                                                 cert_cert)

            # to ensure ssl connect , set grpc env
            os.environ['GRPC_SSL_CIPHER_SUITES'] = 'ECDHE-ECDSA-AES256-GCM-SHA384'

            channel = grpc.secure_channel(endpoints, credentials)
        elif any(cert_params):
            # some of the cert parameters are set
            raise ValueError('the parameters cert_cert, cert_key and ca_cert '
                             'must all be set to use a secure channel')
        else:
            channel = grpc.insecure_channel(endpoints)

        # use aiogrpc to decorate channel
        channel = Channel(channel)

        return channel, credentials

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


def client(endpoints,
           ca_cert=None, cert_key=None, cert_cert=None, timeout=None):

    # user `ip:port,ip:port` to user grpc balance

    return Client(endpoints="ipv4:///" + endpoints, ca_cert=ca_cert, cert_key=cert_key,
                  cert_cert=cert_cert, timeout=timeout)



