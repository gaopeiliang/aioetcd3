import aiogrpc
import os
from aiogrpc.channel import Channel
from aioetcd3.kv import KV
from aioetcd3.lease import Lease
from aioetcd3.auth import Auth
from aioetcd3.watch import Watch
from aioetcd3.maintenance import Maintenance
from aioetcd3.cluster import Cluster
from aioetcd3.utils import get_secure_creds


class Client(KV, Lease, Auth, Watch, Maintenance, Cluster):
    def __init__(self, endpoint, ssl=False,
                 ca_cert=None, cert_key=None, cert_cert=None,
                 default_ca=False, grpc_options = None, timeout=5,
                 *, loop=None, executor=None):
        channel = self._create_grpc_channel(endpoint=endpoint, ssl=ssl,
                                            ca_cert=ca_cert,
                                            cert_key=cert_key, cert_cert=cert_cert,
                                            default_ca=default_ca,
                                            options=grpc_options,
                                            loop=loop,
                                            executor=executor)
        super().__init__(channel, timeout)

    def update_server_list(self, endpoint):
        self.close()
        channel = self._recreate_grpc_channel(endpoint)
        self._update_channel(channel)

    def _create_grpc_channel(self, endpoint, ssl=False,
                             ca_cert=None, cert_key=None, cert_cert=None, default_ca=False, options=None,
                             *, loop=None, executor=None):
        credentials = None
        if not ssl:
            channel = aiogrpc.insecure_channel(endpoint, options=options, loop=loop, executor=executor,
                                               standalone_pool_for_streaming=True)
        else:
            if default_ca:
                ca_cert = None
            else:
                if ca_cert is None:
                    ca_cert = ''

            # to ensure ssl connect , set grpc env
            # os.environ['GRPC_SSL_CIPHER_SUITES'] = 'ECDHE-ECDSA-AES256-GCM-SHA384'

            credentials = aiogrpc.ssl_channel_credentials(ca_cert, cert_key, cert_cert)
            channel = aiogrpc.secure_channel(endpoint, credentials, options=options,
                                             loop=loop, executor=executor,
                                             standalone_pool_for_streaming=True)

        # Save parameters for auto-recreate
        self._credentials = credentials
        self._options = options
        self._loop = channel._loop
        self._executor = executor
        return channel

    def _recreate_grpc_channel(self, endpoint):
        if self._credentials:
            channel = aiogrpc.secure_channel(endpoint, self._credentials, options=self._options,
                                             loop=self._loop, executor=self._executor,
                                             standalone_pool_for_streaming=True)
        else:
            channel = aiogrpc.insecure_channel(endpoint, options=self._options, loop=self._loop,
                                               executor=self._executor, standalone_pool_for_streaming=True)
        return channel
    
    def close(self):
        return self.channel.close()


def client(endpoint, grpc_options=None, timeout=None):

    # user `ip:port,ip:port` to user grpc balance
    return Client(endpoint, grpc_options=grpc_options, timeout=timeout)


def ssl_client(endpoint, ca_file=None, cert_file=None, key_file=None, default_ca=False, grpc_options=None,
               timeout=None):
    ca, key, cert = get_secure_creds(ca_cert=ca_file, cert_cert=cert_file, cert_key=key_file)
    return Client(endpoint, ssl=True, ca_cert=ca, cert_key=key, cert_cert=cert,
                  default_ca=default_ca, grpc_options=grpc_options, timeout=timeout)


def set_grpc_cipher(enable_rsa=True, enable_ecdsa=True, ciphers=None):
    """
    Set GRPC_SSL_CIPHER_SUITES environment variable to change the SSL cipher
    used by GRPC. By default the GRPC C core only supports RSA.

    :param enable_rsa:  Enable RSA cipher
    :param enable_ecdsa: Enable ECDSA cipher
    :param ciphers: Override the cipher list to a list of strings
    """
    if ciphers:
        os.environ['GRPC_SSL_CIPHER_SUITES'] = ':'.join(ciphers)
    else:
        rsa_ciphers = 'ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-SHA256:' \
                      'ECDHE-RSA-AES256-SHA384:ECDHE-RSA-AES256-GCM-SHA384'
        ecdsa_ciphers = 'ECDHE-ECDSA-AES256-GCM-SHA384'
        if enable_rsa:
            if enable_ecdsa:
                env = rsa_ciphers + ':' + ecdsa_ciphers
            else:
                env = rsa_ciphers
        else:
            if enable_ecdsa:
                env = ecdsa_ciphers
            else:
                env = None
        if env is None:
            if 'GRPC_SSL_CIPHER_SUITES' in os.environ:
                del os.environ['GRPC_SSL_CIPHER_SUITES']
        else:
            os.environ['GRPC_SSL_CIPHER_SUITES'] = env
