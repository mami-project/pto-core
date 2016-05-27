from functools import partial

def rangecheck(value, datatype, min=None, max=None, allow_none=False):
    if not isinstance(value, datatype):
        return False

    if not allow_none and value is None:
        return False

    if min is not None and value < min:
        return False

    if max is not None and value > max:
        return False

    return True


checks = {
    "tcp-ttl": partial(rangecheck, datatype=int, min=0, max=255),
    "udp-ttl": partial(rangecheck, datatype=int, min=0, max=255),
    "tcp-rtt": partial(rangecheck, datatype=float, min=0),
    "udp-rtt": partial(rangecheck, datatype=float, min=0),
    "tcp-rtt-max": partial(rangecheck, datatype=float, min=0),
    "udp-rtt-max": partial(rangecheck, datatype=float, min=0),
    "tcp-rtt-min": partial(rangecheck, datatype=float, min=0),
    "udp-rtt-min": partial(rangecheck, datatype=float, min=0),
}