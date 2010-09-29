"""
Usefull utilities for the WW mutants projects
"""

from protolyze.tool.MSMBuilder import Builder, Cluster
from protolyze.weave import MSMBuilder
from protolyze.db.results import WorkqueuePathFixer

from weaver.dataset import MySQLDataSet, Query, And

import os.path
import itertools
import functools


class Project(object):
    def __init__(self, number, hostname, mutant, dbname, dbtype):
        self.number   = number
        self.hostname = hostname
        self.mutant   = mutant
        self.dbname   = dbname
        self.dbtype   = dbtype

class DBAccess(object):
    def __init__(self, db=None, host=None, table=None, user=None, password=None):
        self.db       = db
        self.host     = host
        self.table    = table
        self.user     = user
        self.password = password


def get_projects(dbname = lambda proj_number: 'P10K%02d' % (proj_number - 10000),
                 dbaccess = DBAccess(),
                 dbtype = 'mysql'):

    def mkProject(n,v): return Project(n, dbaccess.host, v, dbname(n), dbtype)

    projects = map(lambda (k,v): mkProject(10000+k, v), [
            (9,14), (12,0), (13,1), (14,2),
            (15, 3), (16,4), (17, 5),
            (19, 7), (20, 8), (21, 9), (22, 10), (23, 11), (24, 12),
            (27,16),
            (30,19), (31,20), (32, 21), (33, 22), (34, 23),
            (38,27), (39, 28)
            ]
                   )

    return projects


def database(project, dbaccess=DBAccess(), file_path = lambda obj: obj.xtc):
    db = MySQLDataSet( host  = project.hostname,
                       name  = project.dbname,
                       table = dbaccess.table,
                       user  = dbaccess.user,
                       pswd  = dbaccess.password )
    db.file_path = lambda obj: obj.xtc
    return db


NDX_ROOT = '/afs/crc.nd.edu/user/c/cabdulwa/Public/Research/md/ww/ndx-msmbuilder'
def ndx(name, project, ndx_root=NDX_ROOT):
    return os.path.join(ndx_root, '%d/%s' % (project.mutant, name))


def get_xtcs(project, frames=None, limit=None, dbaccess=DBAccess()):
    wq_path_fixer = WorkqueuePathFixer()
    db      = database(project, dbaccess=dbaccess)

    predicate = And(db.c.frames == frames, db.c.xtc != None)

    if isinstance(limit, int):
        q = Query(db, predicate, limit=limit ) 
    else:
        q = Query(db, predicate)

    xtcs    = itertools.imap( wq_path_fixer.fix, q )
    return list(xtcs)


def msm_path(msm_root, ndx_name, msm_on=None):
    root = os.path.join(msm_root, ndx_name)
    if isinstance(msm_on, Project):
        typ = 'singleton'
        name = msm_on.dbname
    elif isinstance(msm_on, list) and all(map(lambda o: isinstance(o, project), msm_on)):
        typ = 'combined'
        name = '-'.join(map(lambda p: p.dbname, msm_on))
    else:
        raise TypeError, "msm_path: 'msm_on' kwarg needs to be either a Project or a list of projects"

    return os.path.join(root, typ, name)
