def grouper(iterable, count):
    iterator = iter(iterable)
    while True:
        lst = []
        try:
            for index in range(count):
                lst.append(next(iterator))
        except StopIteration:
            pass
        if len(lst) > 0:
            yield lst
        else:
            break


def grouper_transpose(iterable, count, tuple_length=2):
    for group in grouper(iterable, count):
        newl = []
        for n in range(tuple_length):
            newl.append([tup[n] for tup in group])
        yield newl


def dict_to_sorted_list(obj):
    if isinstance(obj, dict):
        return [dict_to_sorted_list([key, obj[key]]) for key in sorted(obj.keys())]
    elif isinstance(obj, list):
        return [dict_to_sorted_list(elem) for elem in obj]
    else:
        return obj


def rflatten(obj):
    out = []
    for elem in obj:
        if isinstance(elem, list):
            out.append('[')
            out.extend(rflatten(elem))
            out.append(']')
        else:
            out.append(elem)
    return out