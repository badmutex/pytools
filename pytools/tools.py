

def chunkmap(chunksize, fn, seq):
    """
    Integer -> (a -> b) -> Iterable a -> Iterable [b]

    >>> list(chunkmap(5,lambda x: x+10, range(10)))
    [[10, 11, 12, 13, 14], [15, 16, 17, 18, 19]]
    """

    buffer = []
    for val in seq:
        buffer.append( fn(val) )
        if len(buffer) < chunksize: continue
        else:
            yield buffer
            buffer = []
    if buffer: yield buffer


def lazy_chunk(iterable, chunksize):
    if chunksize <= 0:
        chunksize = 1

    import itertools as itr
    return itr.ifilter(bool,
                       itr.izip_longest( *[iter(iterable)]*chunksize,
                                          fillvalue = None  ))
