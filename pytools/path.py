
import os.path

def full(path):
    """
    Returns the full and absolute path after expanding any users
    IE full('~/foo') should return '/home/SomeUser/foo'
    """
    return os.path.abspath( os.path.expanduser(path) )
