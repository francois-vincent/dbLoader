# -*- coding: utf-8 -*-

__version__ = '0.1.1'
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
- downloads all the new products files in a local folder "_host_<host-name>"
"""

from fabric.api import env, task
from fabric.context_managers import cd, prefix
from fabric.operations import put, get, run, sudo
from fabric.contrib import files
from tempfile import NamedTemporaryFile
import os.path
from dbMapLoader import mapDict

REMOTE_WORKING_DIRECTORY = '.dbLoader'

FabContext = mapDict(
    working_dir = REMOTE_WORKING_DIRECTORY,
    sequencer = 'sequencer.py',
    data_file = 'files/MA1.json',
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

def download_new_files(folder):
    res = run('find %s -maxdepth 1 -type f' % folder, warn_only=True)
    if res and not res.failed:
        local = os.path.join('_host_'+env.host, folder)
        if not os.path.exists(local):
            os.makedirs(local)
        for remote in res.split():
            remote = os.path.normpath(remote)
            path = os.path.join((os.path.dirname(local)), remote)
            if not os.path.exists(path):
                get(remote, local)

def download_last_modified(folder):
    res = run('ls %s -1rt | tail -n 1' % folder, warn_only=True)
    if res and not res.failed:
        local = os.path.join('_host_'+env.host, folder)
        if not os.path.exists(local):
            os.makedirs(local)
        remote = os.path.join(folder, res.strip())
        get(remote, local)

def check_params():
    if not env.hosts:
        print "No target defined. Please use option '-H user@host'."
        return
    if not '@' in env.hosts[0]:
        print "No user specified in target. Please use option '-H user@host'."
        return
    print "will connect to", env.host_string

def install_virtualenv():
    # check/create virtualenv '.dbLoader' in ~
    if not files.exists('%(working_dir)s/bin/activate' % FabContext):
        run('virtualenv %(working_dir)s' % FabContext)
    with cd(REMOTE_WORKING_DIRECTORY):
        # create remote virtualenv python installation with requirements (psycopg2, sqlalchemy)
        # and install them, if required only
        with open('requirements.txt') as f:
            dependencies = f.read()
        module_install_list = []
        with prefix('source bin/activate'):
            for module_def in dependencies.split():
                if module_def:
                    parts = module_def.split('==')
                    res = run("python -c 'import %s as toto; print toto.__version__'" % (parts[0],), warn_only=True)
                    if res.failed:
                        module_install_list.append(module_def)
                    if parts[1] and res.split()[0] <> parts[1]:
                        module_install_list.append(module_def)
            if module_install_list:
                upload_data2file('\n'.join(module_install_list), 'reqs.txt')
                run('pip install -r reqs.txt')

def install_engine():
    with cd(REMOTE_WORKING_DIRECTORY):
        # pythor is still not on Pypi ...
        res = run("python -c 'import pythor'", warn_only=True)
        if res.failed:
            import pythor
            put(pythor.__file__, '.')
        #  check/create 'files' subdirectory
        if not files.exists('files'):
            run('mkdir files')
        # upload proper engine
        put('dbMapLoader.py', '.')
        # upload logger and cmd line parser
        put('files/__init__.py', 'files')
        put('files/simpleLogger.py', 'files')
        put('files/json_pp.py', 'files')

def proper_inject():
    with cd(REMOTE_WORKING_DIRECTORY):
        # upload sequencer, data file and connection file
        put(FabContext.sequencer, '.')
        put(FabContext.data_file, 'files')
        upload_data2file(repr(connection), 'connection.json')
        # launch remote injection command
        with prefix('source bin/activate'):
            run('python %(sequencer)s %(data_file)s %(injection_options)s' % FabContext)
            # download last modified result files (log, flat_inj and flat_seq)
        download_last_modified('log')
        download_last_modified('flat_inj')
        download_last_modified('flat_seq')

@task
def inject():
    check_params()
    install_virtualenv()
    install_engine()
    proper_inject()

@task
def inject_only():
    proper_inject()

@task
def restart():
    sudo('service postgresql-8.4 restart')
    run("psql -U postgres -c 'select * from user;'")
