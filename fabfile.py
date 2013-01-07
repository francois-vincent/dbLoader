# -*- coding: utf-8 -*-

__version__ = '0.1.0'
__date__ = 'jan 07th, 2013'
__author__ = 'François Vincent'
__mail__ = 'fvincent@groupeseb.com'
__github__ = 'https://github.com/francois-vincent'

from fabric.api import env, run
from fabric.context_managers import settings, cd, prefix
from fabric.operations import put, get
from tempfile import NamedTemporaryFile
import os.path
from dbMapLoader import mapDict
from cStringIO import StringIO

WORKING_DIRECTORY = '.dbLoader'

dependencies = """
psycopg2==2.4.5
sqlalchemy==0.7.9
"""

test_psycopg2 = """
# -*- coding: utf-8 -*-
import psycopg2
print psycopg2.__version__
"""
test_sqlalchemy = """
# -*- coding: utf-8 -*-
import sqlalchemy
print sqlalchemy.__version__
"""

FabContext = mapDict(
    working_dir = WORKING_DIRECTORY,
    sequencer = 'sequencer.py',
    data_file = 'files/MyActifry.json',
    injection_options = '',
)

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
    # check/create virtualenv '.dbLoader' with subfolder '.dbLoader/files' in ~admin
    with settings(warn_only=True):
        res = run('ls %(working_dir)s/bin/activate' % FabContext)
    if res.failed:
        run('virtualenv %(working_dir)s' % FabContext)
    with cd(WORKING_DIRECTORY):
        #  check/create 'files' subdirectory
        with settings(warn_only=True):
            res = run('ls files')
        if res.failed:
            run('mkdir files')
        with prefix('source bin/activate'):
            # create remote virtualenv python installation (psycopg2, sqlalchemy)
            upload_data2file(dependencies, 'reqs.txt')
#            run('pip install -r reqs.txt')
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
#        with prefix('source bin/activate'):
#            run('python %(sequencer)s files/%(data_file)s %(injection_options)s')
#        # download the log file, prepending hostname_ to log filename
#        with settings(warn_only=True):
#            myout = StringIO()
#            res = run('ls log', stdout=myout)
#        if not res.failed:
#            last_log = sorted(myout.getvalue().split())[-1]
#            get('log/'+last_log, 'log/'+target+'_'+last_log)
