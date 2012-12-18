# -*- coding: utf-8 -*-

__version__ = '0.3.0'
__date__ = 'dec 18th, 2012'
__author__ = 'François Vincent'
__mail__ = 'fvincent@groupeseb.com'
__github__ = 'https://github.com/francois-vincent'

from inspect import stack, getargvalues, getargspec
import new, re
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
            print 'INFO  -', message
    def warning(self, message):
        if self >= 1:
            print 'WARN  -', message
    def error(self, message):
        if self >= 0:
            print 'ERROR -', message

#      Utility functions
# --------------------------
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
            val = int(value)
            h, m = val / 60, val % 60
            return time(h, m)
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
    # TODO synchronize sequencer and injector on flush, and reintroduce failed_list
    mapper = {}
    def __init__(self, log=1, imports=tuple()):
        try:
            self.log = miniLogger(int(log))
        except:
            self.log = log
        self.dbengine = None
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
        self.log.info("SEQUENCER END %s record(s) will be imported (out of %s)"%(self.records_processed, len(self.records)))
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
        try:
            _record_name, _record  = self._get_caller_parameter()
            exec('global {0}; {0} = _record'.format(_record_name.lower()))
        except:
            pass
        mapping = dict()
        for _c, _expr in self.mapper[_table].iteritems():
            _expr = _expr.encode('utf8')
            if _expr.startswith('session'):
                mapping[_c.lower()] = _expr
            else:
                try:
                    mapping[_c.lower()] = eval(_expr)
                except Exception, e:
                    raise RuntimeError("skip <%s>, eval error {%s: <%s>}: %s" % (_table, _c, _expr, e))
        return mapping
    def _eval_expr(self, _expr):
        try:
            return eval(_expr)
        except:
            return _expr
    def select(self, _table, _multiple=False):
        if self.dbengine:
            try:
                _filter = self._eval_mapping(_table)
            except Exception, e:
                self.log.error("  FAILED select <%s>: %s" % (_table, e))
                self.flat.append("comment select failed <%s>" % _table)
                raise SequencerInterruption()
            _obj = self.dbengine.mapper_objects[_table]
            _query = self.orm_session.query(_obj).filter_by(**_filter)
            _count = _query.count()
            if _count and (_multiple or _count==1):
                _obj = ("select "+_table, _filter)
                self._filter = _filter
                self.flat.append(_obj)
                return True
            elif _count:
                self.log.error("  FAILED select <%s>, %d records found, query: %s" % (_table, _count, _filter))
                self.flat.append("comment select failed <%s>" % _table)
                raise SequencerInterruption()
        return False
    def create(self, _table):
        try:
            _filter = self._eval_mapping(_table)
        except Exception, e:
            self.log.error("  FAILED create table <%s>: %s" % (_table, e))
            self.flat.append("comment create failed <%s>" % _table)
            raise SequencerInterruption()
        _obj = ("create "+_table, _filter)
        self.flat.append(_obj)
        if self.dbengine:
            pass
    def update(self, _table, _update_table):
        if self.dbengine:
            try:
                _filter = self._eval_mapping(_table)
            except Exception, e:
                self.log.error("  FAILED update <%s>: %s" % (_table, e))
                self.flat.append("comment update failed <%s>" % _table)
                raise SequencerInterruption()
            _obj = self.dbengine.mapper_objects[_table]
            _query = self.orm_session.query(_obj).filter_by(**_filter)
            _count = _query.count()
            if _count == 1:
                try:
                    _filter2 = self._eval_mapping(_update_table)
                except Exception, e:
                    self.log.error("  FAILED update <%s>: %s" % (_update_table, e))
                    self.flat.append("comment update failed <%s>" % _update_table)
                    raise SequencerInterruption()
                _obj = ("update "+_table, _filter2, _filter)
                self._filter = _filter
                self.flat.append(_obj)
                return True
            elif _count:
                self.log.error("  FAILED update <%s>, %d records found, query: %s" % (_table, _count, _filter))
                self.flat.append("comment update failed <%s>" % _table)
                raise SequencerInterruption()
        return False
    def create_or_select(self, check_table, create_table):
        _return = True
        if self.dbengine:
            try:
                _filter = self._eval_mapping(check_table)
            except Exception, e:
                self.log.error("  FAILED create_or_select check_table: %s"%e)
                raise SequencerInterruption()
            _obj = self.dbengine.mapper_objects[check_table]
            _query = self.orm_session.query(_obj).filter_by(**_filter)
            _count = _query.count()
            if _count == 1:
                _obj = ("select "+create_table, _filter)
                self._filter = _filter
                _table = check_table
                _return = False
            elif not _count:
                _obj = ("create "+create_table, {})
                _table = create_table
            else:
                self.log.error("  FAILED create_or_select <%s>, multiple records found, query: %s" % (check_table, _filter))
                raise SequencerInterruption()
        else:
            _obj = ("create "+create_table, {})
            _table = create_table
        if _table is create_table:
            try:
                _filter = self._eval_mapping(_table)
            except Exception, e:
                self.log.error("  FAILED create_or_select create_table: %s"%e)
                raise SequencerInterruption()
            _obj[1].update(_filter)
        self.flat.append(_obj)
        return _return
    def create_or_skip(self, check_table, create_table):
        if self.dbengine:
            try:
                _filter = self._eval_mapping(check_table)
            except Exception, e:
                self.log.error("  FAILED create_or_skip check_table: %s"%e)
                raise SequencerInterruption()
            _obj = self.dbengine.mapper_objects[check_table]
            _query = self.orm_session.query(_obj).filter_by(**_filter)
            if _query.count():
                self.log.info('  TABLE SKIPPED <%s>'%create_table)
                return False
            else:
                _obj = ("create "+create_table, {})
                _table = create_table
        else:
            _obj = ("create "+create_table, {})
            _table = create_table
        if _table is create_table:
            try:
                _filter = self._eval_mapping(_table)
            except Exception, e:
                self.log.error("  FAILED create_or_skip create_table: %s"%e)
                raise SequencerInterruption()
        _obj[1].update(_filter)
        self.flat.append(_obj)
        return True
    def create_or_update(self, check_table, create_table, update_table):
        _return = True
        if self.dbengine:
            try:
                _filter = self._eval_mapping(check_table)
            except Exception, e:
                self.log.error("  FAILED create_or_update check_table: %s"%e)
                raise SequencerInterruption()
            _obj = self.dbengine.mapper_objects[check_table]
            _query = self.orm_session.query(_obj).filter_by(**_filter)
            _count = _query.count()
            if _count == 1:
                # Warn: this is the only case where record is a triple
                _obj = ("update "+create_table, {}, _filter)
                self._filter = _filter
                _table = update_table
                _return = False
            elif not _count:
                _obj = ("create "+create_table, {})
                _table = create_table
            else:
                self.log.error("  FAILED create_or_update <%s>, multiple records found, query: %s" % (check_table, _filter))
                raise SequencerInterruption()
        else:
            _obj = ("create "+create_table, {})
            _table = create_table
        try:
            _filter = self._eval_mapping(_table)
        except Exception, e:
            self.log.error("  FAILED create_or_update create_table: %s"%e)
            raise SequencerInterruption()
        _obj[1].update(_filter)
        self.flat.append(_obj)
        return _return
    def commit(self):
        self.flat.append("commit")
    def flush(self):
        self.flat.append("flush")
    def comment(self, message):
        self.flat.append("comment "+message)
    def abort(self):
        raise SequencerInterruption()
    def cancel_last(self):
        if self.flat:
            self.flat.pop()

class Shell(object):
    def __init__(self, sequencer, injector=None):
        self.sequencer = sequencer
        self.injector = injector
        if injector:
            self.sequencer.dbengine = self.injector.engine
            self.sequencer.session = self.injector.session


class dbLoader(object):
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
            message += kwargs['connection'].get('error', '')%kwargs['connection']['connection_context']
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
    def prepare_injection(self, records):
        rec_num = len([x for x in records])
        rec_num_get = len([x for x in records if isinstance(x, tuple) and x[0].startswith('select')])
        rec_num_set = len([x for x in records if isinstance(x, tuple) and x[0].startswith('create')])
        rec_num_update = len([x for x in records if isinstance(x, tuple) and x[0].startswith('update')])
        self.log.info("INJECT START %d record(s), %d select, %d create, %d update" % (rec_num, rec_num_get, rec_num_set, rec_num_update))
        self.create_count, self.update_count = 0, 0
    def inject_many(self, records):
        self.prepare_injection(records)
        for record in records:
            if not self.inject_one(record):
                self.log.error("INJECT ABORTED @ record %d" % self.create_count)
                return False
        self.log.info("INJECT END OK")
        return True
    def _flush_flat(self):
        for _command, _table, _obj in self.to_flush:
            _cols = {}
            for _col in self.get_columnames(_table.split(':')[0]):
                _cols[_col] = getattr(_obj, _col)
            self.flat.append((_command+_table, _cols))
        self.to_flush = []
    def create(self, _table, _record):
        session = self.session
        self.create_count += 1
        self.log.info("INJECT create step %d record <%s>" % (self.create_count, _table))
        try:
            _obj = self.mapper_objects[_table]()
            for _key, _value in _record.iteritems():
#                if isinstance(_value, basestring) and _value.startswith('session'):
#                    _value = eval(_value)
                _obj.__setattr__(_key, _value)
            session[_table] = _obj
            self.to_flush.append(('create ', _table, _obj))
            self.orm_session.add(_obj)
        except Exception, e:
            self.orm_session.rollback()
            self.log.error('INJECT ABORTED create <%s>: %s'%(_table, e))
            return False
    def select(self, _table, _record):
        session = self.session
        self.log.info("INJECT select record <%s>" %  _table)
        _mapper_object = self.mapper_objects[_table]
        _filter = _record
        _query = self.orm_session.query(_mapper_object).filter_by(**_filter)
        try:
            _object = session[_table] = _query.one()
        except:
            self.orm_session.rollback()
            self.log.error('INJECT ABORTED select <%s> multiple instances query %s'%(_table, _filter))
            return False
        _cols = dict((_col, getattr(_object, _col)) for _col in self.get_columnames(_table.split(':')[0]))
        self.flat.append(('get '+_table, _cols))
    def update(self, _table, _record, _record2):
        session = self.session
        self.update_count += 1
        self.log.info("INJECT update step %d record <%s>" % (self.update_count, _table))
        _mapper_object = self.mapper_objects[_table]
        _filter = _record2
        _query = self.orm_session.query(_mapper_object).filter_by(**_filter)
        try:
            _object = session[_table] = _query.one()
        except:
            self.orm_session.rollback()
            self.log.error('INJECT ABORTED update <%s> multiple instances query %s'%(_table, _filter))
            return False
        try:
            _updated = False
            for _key, _value in _record[1].iteritems():
                if isinstance(_value, basestring) and _value.startswith('session'):
                    _value = eval(_value)
                if getattr(_object, _key) != _value:
                    _object.__setattr__(_key, _value)
                    _updated = True
            session[_table] = _object
            if not _updated:
                _object = None
            self.to_flush.append(('update ', _table, _object))
        except Exception, e:
            self.orm_session.rollback()
            self.log.error('INJECT ABORTED create <%s> exception: %s'%(_record[0], e))
            return False
    def inject_one(self, _record):
        if isinstance(_record, tuple):
            _split_command = _record[0].split()
            _table = _split_command[1]
            if _split_command[0] == "create":
                return self.create(_table, _record[1])
            elif _split_command[0] == "select":
                return self.select(_table, _record[1])
            elif _split_command[0] == "update":
                return self.update(_table, _record[1], _record[2])
        else:
            _split_command = _record.split()
            if _split_command[0]=="flush":
                self.orm_session.flush()
            elif _split_command[0]=="commit":
                self.orm_session.commit()
                self._flush_flat()
            elif len(_split_command)==2 and _split_command[0]=="run":
                try:
                    message = []
                    result = getattr(translators, _split_command[1])(self, message)
                    if result > 1:
                        self.orm_session.rollback()
                        self.log.error('INJECT ABORTED processor <%s>: %s'%(_split_command[1], message[0]))
                        return False
                except Exception, e:
                    self.orm_session.rollback()
                    self.log.error('INJECT ABORTED processor <%s> exception: %s'%(_split_command[1], e))
                    return False
            elif _split_command[0]=="comment":
                pass # There is really nothing to do here !!
            else:
                self.log.error('INJECT ABORTED unknown command <%s>'%_record)
                return False
        return True
