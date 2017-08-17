from aioetcd3.utils import increment_last_byte, to_bytes

SORT_ASCEND = 'ascend'
SORT_DESCEND = 'descend'


def range_prefix(key):
    return key, increment_last_byte(to_bytes(key))


def range_greater(key):
    return key, b'\0'


def range_less(key):
    pass


def range_all():
    return b'\0', b'\0'