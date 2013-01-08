# -*- coding: utf-8 -*-

__version__ = '0.1.0'
__date__ = 'jan 07th, 2013'
__author__ = 'François Vincent'
__mail__ = 'fvincent@groupeseb.com'
__github__ = 'https://github.com/francois-vincent'

from fabric.api import env
from fabric.context_managers import cd, prefix
from fabric.operations import put, get, run
from tempfile import NamedTemporaryFile
import os.path
from dbMapLoader import mapDict

WORKING_DIRECTORY = '.dbLoader'

FabContext = mapDict(
    working_dir = WORKING_DIRECTORY,
    sequencer = 'sequencer.py',
    data_file = 'files/MyActifry.json',
    injection_options = '',
)

dependencies = """
psycopg2==2.4.5
sqlalchemy==0.7.9
"""

connection = {
    "connection_context" : {
        "name":   "dbanalytic",
        "user":   "devseb",
        "passwd": "devseb",
        "type":   "postgresql",
        "host":   "127.0.0.1",
        "port":   "5432"
    },
    "dsn" : "%(type)s://%(user)s:%(passwd)s@%(host)s:%(port)s/%(name)s",
    "schema" : "sch_dbanalytic",
}

def upload_data2file(data, remote_filename):
    with NamedTemporaryFile() as temp_file:
        temp_file.write(data.lstrip())
        temp_file.flush()
        put(temp_file.name, remote_filename)

def inject():
    if not env.hosts:
        print "No target defined. Please use option '-H user@host'."
        return
    if not '@' in env.hosts[0]:
        print "No user specified in target. Please use option '-H user@host'."
        return
    print "will connect to", env.hosts
    target = env.hosts[0].split('@')[1]
    # check/create virtualenv '.dbLoader' with subfolder 'files' in ~admin
    res = run('ls %(working_dir)s/bin/activate' % FabContext, warn_only=True)
    if res.failed:
        run('virtualenv %(working_dir)s' % FabContext)
    with cd(WORKING_DIRECTORY):
        #  check/create 'files' subdirectory
        res = run('ls files', warn_only=True)
        if res.failed:
            run('mkdir files')
        with prefix('source bin/activate'):
            # create remote virtualenv python installation (psycopg2, sqlalchemy)
            upload_data2file(dependencies, 'reqs.txt')
            run('pip install -r reqs.txt')
        # upload classes, sequencer and connection parameters
        put('dbMapLoader.py', 'dbMapLoader.py')
        put(FabContext.sequencer, FabContext.sequencer)
        upload_data2file(repr(connection), 'connection.json')
        # upload logger
        put('files/__init__.py', 'files/__init__.py')
        put('files/simpleLogger.py', 'files/simpleLogger.py')
        # upload data file
        put(FabContext.data_file, 'files/'+os.path.basename(FabContext.data_file))
        # launch remote injection command
        with prefix('source bin/activate'):
            run('python %(sequencer)s files/%(data_file)s %(injection_options)s')
        # download result files (log, flat_inj et flat_seq)
        res = run('find log -maxdepth 1 -type f', warn_only=True)
        if res and not res.failed:
            local = os.path.join('_host_'+target, 'log')
            if not os.path.exists(local):
                os.makedirs(local)
            remote = sorted(res.split())[-1]
            remote = os.path.normpath(remote)
            get(remote, local)
        res = run('find flat_inj -maxdepth 1 -type f', warn_only=True)
        if res and not res.failed:
            local = os.path.join('_host_'+target, 'flat_inj')
            if not os.path.exists(local):
                os.makedirs(local)
            for remote in res.split():
                remote = os.path.normpath(remote)
                path = os.path.join(local, remote)
                if not os.path.exists(path):
                    get(remote, local)
        res = run('find flat_seq -maxdepth 1 -type f', warn_only=True)
        if res and not res.failed:
            local = os.path.join('_host_'+target, 'flat_seq')
            if not os.path.exists(local):
                os.makedirs(local)
            for remote in res.split():
                remote = os.path.normpath(remote)
                path = os.path.join(local, remote)
                if not os.path.exists(path):
                    get(remote, local)
