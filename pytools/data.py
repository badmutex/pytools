
import itertools

class Either(object):

    left = None # failure
    right = None # success

    def success(self):
        return self.right is not None

    def failure(self):
        return self.left is not None

def Left(obj):
    e = Either()
    e.left = obj
    return e

def Right(obj):
    e = Either()
    e.right = obj
    return e

def rights(eithers):
    return itertools.imap(lambda e: e.right, itertools.ifilter(lambda e: e.success(), eithers))

def lefts(eithers):
    return itertools.imap(lambda e: e.left,  itertools.ifilter(lambda e: e.failure(), eithers))


def partition(eithers):
    """
    Returns a tuple of generators ([left value], [right value])
    """

    return lefts(eithers), rights(eithers)
