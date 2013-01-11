# -*- coding: utf-8 -*-

__version__ = '0.1.0'
__date__ = 'jan 07th, 2013'
__author__ = 'François Vincent'
__mail__ = 'fvincent@groupeseb.com'
__github__ = 'https://github.com/francois-vincent'

"""
This is a fabric script that launches injection directly onto the remote target host
(x5-7 speed up for big data set)
This script:
- creates a python virtualenv and installs the required versions of psycopg2 and sqlalchemy
- uploads all the required files (python scripts, data files)
- run the mapper/sequencer on the remote target
- downloads all the products files
"""

from fabric.api import env, task
from fabric.context_managers import cd, prefix
from fabric.operations import put, get, run
from fabric.contrib import files
from tempfile import NamedTemporaryFile
import os.path
from dbMapLoader import mapDict

WORKING_DIRECTORY = '.dbLoader'

FabContext = mapDict(
    working_dir = WORKING_DIRECTORY,
    sequencer = 'sequencer.py',
    data_file = 'files/MA1.json',
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

def download_new_files(folder):
    res = run('find %s -maxdepth 1 -type f' % folder, warn_only=True)
    if res and not res.failed:
        local = os.path.join('_host_'+env.host, folder)
        if not os.path.exists(local):
            os.makedirs(local)
        for remote in res.split():
            remote = os.path.normpath(remote)
            path = os.path.join(local, remote)
            if not os.path.exists(path):
                get(remote, local)

@task
def inject():
    if not env.hosts:
        print "No target defined. Please use option '-H user@host'."
        return
    if not '@' in env.hosts[0]:
        print "No user specified in target. Please use option '-H user@host'."
        return
    print "will connect to", env.host_string
    # check/create virtualenv '.dbLoader' in ~
    if not files.exists('%(working_dir)s/bin/activate' % FabContext):
        run('virtualenv %(working_dir)s' % FabContext)
    with cd(WORKING_DIRECTORY):
        #  check/create 'files' subdirectory
        if not files.exists('files'):
            run('mkdir files')
        # create remote virtualenv python installation (psycopg2, sqlalchemy)
        with prefix('source bin/activate'):
            upload_data2file(dependencies, 'reqs.txt')
            run('pip install -r reqs.txt')
        # upload classes, sequencer and connection parameters
        put('dbMapLoader.py', '.')
        put(FabContext.sequencer, '.')
        upload_data2file(repr(connection), 'connection.json')
        # upload logger and cmd line parser
        put('files/__init__.py', 'files')
        put('files/simpleLogger.py', 'files')
        put('files/pythor.py', 'files')
        # upload data file
        put(FabContext.data_file, 'files')
        # launch remote injection command
        with prefix('source bin/activate'):
            run('python %(sequencer)s %(data_file)s %(injection_options)s' % FabContext)
        # download result files (log, flat_inj and flat_seq)
        # download last log file
        res = run('find log -maxdepth 1 -type f', warn_only=True)
        if res and not res.failed:
            local = os.path.join('_host_'+env.host, 'log')
            if not os.path.exists(local):
                os.makedirs(local)
            # only get the last one
            remote = sorted(res.split())[-1]
            remote = os.path.normpath(remote)
            get(remote, local)
        # download all new flat files
        download_new_files('flat_inj')
        download_new_files('flat_seq')
