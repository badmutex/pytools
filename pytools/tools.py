

def lazy_chunk(iterable, chunksize):
    if chunksize <= 0:
        chunksize = 1

    buf = []
    for val in iterable:
        buf.append(val)
        if len(buf) >= chunksize:
            yield buf
            buf = []
    if buf:
        yield buf


def lazy_concatMap(fn, *iterables):
    """
    yields the application of *fn* over each value in **iterables*
    """

    for it in iterables:
        for v in it:
            yield fn(v)
