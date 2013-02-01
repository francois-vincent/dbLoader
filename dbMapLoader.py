# -*- coding: utf-8 -*-

__version__ = '0.3.1'
__date__ = 'dec 18th, 2012'
__author__ = 'François Vincent'
__mail__ = 'fvincent@groupeseb.com'
__github__ = 'https://github.com/francois-vincent'

from inspect import stack, getargvalues, getargspec
import new, re, sys
from datetime import time
from sqlalchemy.engine import create_engine, reflection
from sqlalchemy import MetaData, Table, orm

#     Utility Classes
# ------------------------
class miniLogger(int):
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        return
    def info(self, message):
        if self >= 2:
            print >>sys.stderr, 'INFO  -', message
    def warning(self, message):
        if self >= 1:
            print >>sys.stderr, 'WARN  -', message
    def error(self, message):
        if self >= 0:
            print >>sys.stderr, 'ERROR -', message

class mapDict(dict):
    "This interface class allows to map attribute access API and call API to dict element access API"
    __call__ = __getattr__ = dict.__getitem__

#        Decorators
# ------------------------
def _records(func):
    def toto(self, records):
        self._start_records(records)
        try:
            func(self, records)
            r = True
        except SequencerInterruption:
            if self.dbengine:
                self.dbengine.orm_session.rollback()
            self.log.error("SEQUENCER ABORTED")
            r = False
        except InjectorInterruption:
            self.dbengine.orm_session.rollback()
            self.log.error("INJECTOR ABORTED")
            r = False
        self._end_records()
        return r
    return toto

def _record(func):
    def toto(self, record):
        self._start_record()
        if hasattr(self, 'level'):
            self.level += 1
            r = func(self, mapDict(record))
            self.level -= 1
        else:
            r = func(self, mapDict(record))
        self._end_record()
        if hasattr(self, 'records_processed'):
            self.records_processed += 1
        return r
    return toto

def _sub_records(func):
    def toto(self, records):
        self._start_sub_records(records, func)
        r = func(self, records)
        self._end_sub_records()
        return r
    return toto

def _sub_record(func):
    def toto(self, record):
        if hasattr(self, 'level'):
            self.level += 1
            r = func(self, mapDict(record))
            self.level -= 1
        else:
            r = func(self, mapDict(record))
        return r
    return toto

#      Translators
# -----------------------
class translators:
    """
    Translators are functions that can be called from the sequencer via the mapping.
    Purpose is to convert data from imported records
    """
    @ staticmethod
    def set_static(att):
        "allows to add a static method to translators"
        setattr(translators, att.__name__, staticmethod(att))
    @ staticmethod
    def convert_duration(value):
        """converts an integer or a string like 'xx h yy min' to time,
        with great flexibility"""
        try:
            return time(*divmod(int(value), 60))
        except:
            if isinstance(value, basestring):
                x = re.compile(r'(\d+)\s*(\D*)').findall(value)
                if len(x) == 1:
                    group = x[0]
                    if group[1][0].lower() == 'h':
                        return time(int(group[0]), 0)
                    else:
                        return time(0, int(group[0]))
                if len(x) > 2:
                    x = x[:2]
                _time = [0, 0]
                for i, group in enumerate(x):
                    _time[i] = int(group[0])
                return time(*tuple(_time))
            return time(0, 0)

#        Main classes
# -------------------------
class SequencerInterruption(Exception):
    pass

class MapperSequencer(object):
    """
    This is the class from which you will derive your mapper / sequencer
    """
    # TODO reintroduce failed_list
    mapper = {}
    def __init__(self, log=1, imports=tuple()):
        try:
            self.log = miniLogger(int(log))
        except:
            self.log = log
        self.dbengine = None
        self.session = None
        self.flat = []
        self.records_processed = 0
        self.level = -1
        for imp in imports:
            if imp[1]:
                exec('global {0}; global {1}; from {0} import {1}'.format(imp[0], ','.join(imp[1])))
            else:
                exec('global {0}; import {0}'.format(imp[0]))
    def _get_caller_parameter(self, pos=1, pattern='process_', func=None):
        if func:
            return getargspec(func).args[pos]
        depth, func_name = 1, ''
        argInfo = getargvalues(stack()[depth][0])
        while not func_name.startswith(pattern):
            argInfo = getargvalues(stack()[depth][0])
            func_name = stack()[depth][3]
            depth += 1
        return argInfo.args[pos], argInfo.locals[argInfo.args[pos]]
    def _start_records(self, records):
        self.records = records
        if self.dbengine:
            self.orm_session = self.dbengine.orm_session
        self.log.info("SEQUENCER START %d record(s)"%len(records))
    def _end_records(self):
        self.log.info("SEQUENCER END %s record(s) have been imported (out of %s)"%(self.records_processed, len(self.records)))
    def _start_record(self):
        self.log.info("RECORD START %s out of %s"%(self.records_processed+1, len(self.records)))
    def _end_record(self):
        self.log.info("RECORD END %s out of %s"%(self.records_processed+1, len(self.records)))
    def _start_sub_records(self, records, func):
        records_name = self._get_caller_parameter(func=func)
        self.log.info("SUB-RECORDS START %d sub-records (%s) @ level %d"%(len(records), records_name, self.level+1))
    def _end_sub_records(self):
        self.log.info("SUB-RECORD END @ level %d"%(self.level+1,))
    def _eval_mapping(self, _table):
        "returns an evaluated record from the mapper"
        if not isinstance(_table, basestring):
            return _table
        try:
            _record_name, _record = self._get_caller_parameter()
            exec('global {0}; {0} = _record'.format(_record_name.lower()))
        except:
            pass
        session = self.session
        mapping = dict()
        for _c, _expr in self.mapper[_table].iteritems():
#            _expr = _expr.encode('utf8')
            if _expr.startswith('session'):
                _expr = _expr.lower()
            try:
                if _expr.startswith('session') and not self.dbengine:
                    mapping[_c.lower()] = _expr
                else:
                    mapping[_c.lower()] = eval(_expr)
            except Exception, e:
                raise RuntimeError("skip <%s>, eval error {%s: <%s>}: %s" % (_table, _c, _expr, e))
        return mapping
    def create(self, _create):
        """
        _create: a mapping record to inject into the database
        """
        # eval the mapping
        try:
            _filter = self._eval_mapping(_create)
        except Exception, e:
            self.log.error("  FAILED create table <%s>: %s" % (_create, e))
            self.flat.append("comment create failed <%s>" % _create)
            raise SequencerInterruption()
        # save the evaluated record in the flat structure
        self.flat.append(("create "+_create, _filter))
        # if an engine is declared, inject the record into the database
        if self.dbengine:
            _prefix = _create.split(':')[0]
            self.dbengine.create(_prefix, _filter)
    def exists(self, _check):
        return self.select(_check, _check_only=True)
    def select(self, _check, _check_only=False):
        """
        if an injector exists, will set a table in the session according to _check
        _check: a mapping record to match 1 (or more if _check_only is True) database record
        _check_only: if set, this method will allow multiple matching records and will not update session
        returns: # record(s) found
        """
        if self.dbengine:
#            _prefix = _check.split(':')[0]
            try:
                _filter = self._eval_mapping(_check)
            except Exception, e:
                self.log.error("  FAILED select <%s>: %s" % (_check, e))
                self.flat.append("comment select failed <%s>" % _check)
                raise SequencerInterruption()
            _obj = self.dbengine.mapper_objects[_check]
            _query = self.orm_session.query(_obj).filter_by(**_filter)
            _count = _query.count()
            if _count and (_check_only or _count==1):
                self.flat.append(("select "+_check, _filter))
                if not _check_only:
                    self.dbengine.select(_check, _query.one())
                return _count
            elif _count:
                self.log.error("  FAILED select <%s>, %d records found, query: %s" % (_check, _count, _filter))
                self.flat.append("comment select failed <%s>" % _check)
                raise SequencerInterruption()
        return 0
    def update(self, _check, _check_update, _update):
        """
        _check: a mapping record to check exitence of database record
        _check_update: True, False or a mapping record to check if database record must be updated
        _update: a mapping record to update the database record
        returns: 0 (no record found), 1 (record found / not updated), 2 (record found / updated)
        """
        if self.dbengine:
            _mapping_table = _check.split(':')[0]
            if (isinstance(_check_update, basestring) and _check_update.split(':')[0] != _mapping_table) or \
               _update.split(':')[0] != _mapping_table:
                self.log.error("  FAILED update <%s>: tables must have same prefixes" % (_check, ))
                self.flat.append("comment update failed <%s>" % _check)
                raise SequencerInterruption()
            try:
                check_filter = self._eval_mapping(_check)
            except Exception, e:
                self.log.error("  FAILED update <%s>: %s" % (_check, e))
                self.flat.append("comment update failed <%s>" % _check)
                raise SequencerInterruption()
            _obj = self.dbengine.mapper_objects[_check]
            _query = self.orm_session.query(_obj).filter_by(**check_filter)
            _count = _query.count()
            if _count == 1:
                try:
                    _check_update_filter = self._eval_mapping(_check_update)
                except Exception, e:
                    self.log.error("  FAILED update <%s>: %s" % (_check_update, e))
                    self.flat.append("comment update failed <%s>" % _check_update)
                    raise SequencerInterruption()
                try:
                    _update_filter = self._eval_mapping(_update)
                except Exception, e:
                    self.log.error("  FAILED update <%s>: %s" % (_update, e))
                    self.flat.append("comment update failed <%s>" % _update)
                    raise SequencerInterruption()
                self.flat.append(("update "+_check, check_filter))
                return 1+int(self.dbengine.update(_mapping_table, _check_update_filter, _update_filter, _query.one()))
            elif _count:
                self.log.error("  FAILED update <%s>, %d records found, query: %s" % (_check, _count, check_filter))
                self.flat.append("comment update failed <%s>" % _check)
                raise SequencerInterruption()
        return 0
    def commit(self):
        self.flat.append("commit")
        if self.dbengine:
            self.dbengine.commit()
    def flush(self):
        self.flat.append("flush")
        if self.dbengine:
            self.dbengine.flush()
    def rollback(self):
        self.flat.append("Roll Back")
        if self.dbengine:
            self.dbengine.rollback()
    def comment(self, message):
        self.flat.append("comment "+message)
    def abort(self, message):
        self.log.error("ABORT: "+message)
        self.flat.append("comment ABORT: "+message)
        raise SequencerInterruption()
    def cancel_last(self):
        if self.flat:
            self.flat.pop()

class Shell(object):
    def __init__(self, sequencer, injector=None):
        self.sequencer = sequencer
        self.injector = injector
        if injector:
            sequencer.dbengine = injector
            sequencer.session = injector.session
            injector.sequencer = sequencer

class InjectorInterruption(Exception):
    pass

class Injector(object):
    def __init__(self, connection_dsn, kwargs, log=0):
        """
        The constructor establishes the connection to the database and collects some
        basic information like table names.
        connection_dsn (data source name) is a string as used by sqlalchemy's function create_engine()
        log is an integer or a logger instance
        """
        self.dsn = connection_dsn
        if isinstance(log, int):
            self.log = miniLogger(log)
        else:
            self.log = log
        try:
            engine = create_engine(connection_dsn)
            # get a reflection engine instance
            insp = reflection.Inspector.from_engine(engine)
        except Exception, e:
            message = "Couldn't connect to <%s>: %s" % (connection_dsn, e)
            message += kwargs['connection'].get('error', '') % kwargs['connection']['connection_context']
            raise RuntimeError(message)
        self.engine = engine
        self.inspector = insp
        # check presence of required schema in database and store it
        schema = kwargs['connection']['schema']
        if schema and schema not in insp.get_schema_names():
            raise RuntimeError("Required schema %s not found"%schema)
        self.schema = schema
        self.kwargs = kwargs
        # store list of table names
        self.tablenames = [t.lower() for t in insp.get_table_names(schema=schema, order_by='foreign_key')]
        # set this callable for use as a key for sorting tables along dependency order
        self.table_key = mapDict(zip(self.tablenames, xrange(len(self.tablenames))))
        # store metadata
        self.meta = MetaData(engine, schema=schema)
        self.log.info('Connected to base <%s>, schema <%s>'%(connection_dsn, schema))
    def check_mapping(self, mapper):
        """
        mapper is a 2D dict with table names as 1st level keys and column names
        as 2nd level keys.
        This method will check all table and column names specified in mapper
        against what actually lies in the database
        All tables and column names are converted to lower case
        """
        check_ok = True
        name = self.kwargs['connection']['connection_context']['name']
        self.log.info("CHECKING START mapping against base <%s>, schema <%s>" % (name, self.schema))
        for t in mapper:
            tablename = t.split(':')[0].lower()
            if tablename in self.tablenames:
                columnames = [c['name'] for c in self.inspector.get_columns(tablename, self.schema)]
                for col in mapper[t]:
                    if col.lower() not in columnames:
                        self.log.error( "  Column <%s> not in <%s.%s.%s>" % (col, name, self.schema, t))
                        check_ok = False
            else:
                self.log.error("  Table <%s> not in <%s.%s>" % (t, name, self.schema))
                check_ok = False
        if check_ok:
            self.log.info("CHECKING SUCCESS mapping")
        else:
            self.log.error("CHECKING FAILED mapping")
        return check_ok
    def get_tablenames(self):
        return self.tablenames
    def get_columnames(self, table):
        return [c['name'] for c in self.inspector.get_columns(table, schema=self.schema)]
    def prepare_session(self, mapper):
        self.log.info("INJECT SESSION PREPARE")
        self.mapper_objects = dict([(_t, new.classobj(_t.split(':')[0], (object,), {})) for _t in mapper])
        for _t in mapper:
            orm.mapper(self.mapper_objects[_t], Table(_t.split(':')[0], self.meta, autoload=True))
        self.orm_session = orm.sessionmaker(bind=self.engine)()
        self.session = mapDict()
        self.flat = []
        self.to_flush = []
        return self
    def prepare_injection(self, records=None):
        if records:
            rec_num = len(records)
            rec_num_get = len([x for x in records if isinstance(x, tuple) and x[0].startswith('select')])
            rec_num_set = len([x for x in records if isinstance(x, tuple) and x[0].startswith('create')])
            rec_num_update = len([x for x in records if isinstance(x, tuple) and x[0].startswith('update')])
            self.log.info("INJECT START %d record(s), %d select, %d create, %d update" % (rec_num, rec_num_get, rec_num_set, rec_num_update))
        else:
            self.log.info("INJECT START on the fly")
        self.create_count, self.update_count = 0, 0
    def _flush_flat(self):
        for _command, _table, _obj in self.to_flush:
            if isinstance(_obj, dict):
                _cols = _obj
            else:
                _cols = dict((_col, getattr(_obj, _col)) for _col in self.get_columnames(_table.split(':')[0]))
            self.flat.append((_command+_table, _cols))
        self.to_flush = []
    def create(self, _table, _record):
        self.create_count += 1
        self.log.info("INJECT create step %d record <%s>" % (self.create_count, _table))
        try:
            _obj = self.mapper_objects[_table]()
            for _key, _value in _record.iteritems():
                _obj.__setattr__(_key, _value)
            self.session[_table] = _obj
            self.to_flush.append(('create ', _table, _obj))
            self.orm_session.add(_obj)
        except Exception, e:
            self.log.error('INJECT ABORTED create <%s>: %s' % (_table, e))
            raise InjectorInterruption()
    def select(self, _table, _object):
        self.session[_table] = _object
        self.to_flush.append(('get ', _table, _object))
    def update(self, _table, _check_update, _update, _object):
        self.update_count += 1
        self.session[_table] = _object
        try:
            if isinstance(_check_update, bool):
                _must_update = _check_update
            else:
                _must_update = False
                for _key, _value in _check_update.iteritems():
                    if type(_value) is str:
                        _value = _value.decode('utf8')
                    if getattr(_object, _key) != _value:
#                        print '-'*80
#                        print _table, self.sequencer.records_processed
#                        print _key
#                        print type(getattr(_object, _key)), type(_value)
#                        print '<%s>'%getattr(_object, _key)
#                        print '<%s>'%_value
                        _must_update = True
                        break
            if _must_update:
                for _key, _value in _update.iteritems():
                    _object.__setattr__(_key, _value)
                self.to_flush.append(('update ', _table, _update))
            return _must_update
        except Exception, e:
            self.log.error('INJECT ABORTED update <%s> exception: %s' % (_table, e))
            raise InjectorInterruption()
    def commit(self):
        try:
            self.orm_session.commit()
        except Exception, e:
            self.log.error('INJECT ABORTED at commit: '+str(e))
            raise InjectorInterruption()
        self._flush_flat()
    def flush(self):
        try:
            self.orm_session.flush()
        except Exception, e:
            self.log.error('INJECT ABORTED flush')
            raise InjectorInterruption()
    def rollback(self):
        try:
            self.orm_session.rollback()
        except Exception, e:
            self.log.error('INJECT ABORTED rollback')
            raise InjectorInterruption()
