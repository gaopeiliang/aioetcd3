# All of the custom errors are inherited from the grpc.RpcError
# for the backward compatibility
from grpc import RpcError, StatusCode


class EtcdError(RpcError):
    code = StatusCode.UNKNOWN

    def __init__(self, details, debug_info=None):
        self.details = details
        self.debug_info = debug_info

    def __repr__(self):
        return "`{}`: reason: `{}`".format(self.code, self.details)


class AuthError(EtcdError):
    code = StatusCode.INVALID_ARGUMENT


class Unauthenticated(EtcdError):
    code = StatusCode.UNAUTHENTICATED


class InvalidArgument(EtcdError):
    code = StatusCode.INVALID_ARGUMENT


class PermissionDenied(EtcdError):
    code = StatusCode.PERMISSION_DENIED


class FailedPrecondition(EtcdError):
    code = StatusCode.FAILED_PRECONDITION


STATUS_MAP = {
    StatusCode.UNAUTHENTICATED: Unauthenticated,
    StatusCode.PERMISSION_DENIED: PermissionDenied,
    StatusCode.FAILED_PRECONDITION: FailedPrecondition,
}
