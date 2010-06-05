

def chunkmap(chunksize, fn, seq):
    """
    Integer -> (a -> b) -> Iterable a -> Iterable [b]

    >>> list(chunkmap(5,lambda x: x+10, range(10)))
    [[10, 11, 12, 13, 14], [15, 16, 17, 18, 19]]
    """

    bufferseq = []
    for val in seq:
        bufferseq.append(val)
        if len(bufferseq) < chunksize: continue
        else:
            yield map(fn, bufferseq)
            bufferseq = []
    if bufferseq: yield map(fn, bufferseq)
