# dbLoader

This python application/module will help you import compound (nested) data into any kind of relational
database supported by SQLAlchemy.

It is composed of a python module **dbMapLoader.py** and an python example application **sequencer.py**.

**sequencer.py** accepts as input a JSON file that is a list of _compound_ records and injects them into
the specified database. It produces 2 different files: a post sequencer flat file and a post injection
flat file. Detailed operations of import process is logged in a log file.


## Standalone usage

	  sequencer.py records_json [options]

	  This application will help you inject a set of compound records into any kind
	  of relational database supported by SQLAlchemy.

	Options:
	--check-only     | -c (default=False)
	--sequencer-only | -s (default=False)
	--no-check       | -n (default=False)
	--database       | -d <type 'str'> (default='connection.json')
	--eval           | -e (default=False)
	--abort-on-error | -a (default=False)

--check--only: check that the specified mapping is compliant with the database.

--sequencer-only: dry-run the sequencer without a database.

--eval: will evaluate the input records via python interpreter's eval() function instead of json.load()


## Description

The main concepts at work here are:

1. Mapping. Defines the mapping of the database tables/columns on the fields from the imported records or
   on any other data source. See _Mapping_ section below.

2. Sequencing. Defines the logical sequence of the import process, in terms of operations ans loops.
   See _Sequencer_ section below.

3. Reflection. The schema of the database is not an input of the process. Instead, thanks to the reflection
   (introspection) capabilities of SQLAlchemy, the schema is automatically induced from the database itself.

4. 2-phases injection. The import process is done in 2 phases. First phase is _mapping/sequencing_ and
   consists in unrolling the compound records to produce a flat structure in which all fields have been
   evaluated from the input records and from the database when possible.
   Second phase is _injection_ and consists in actually importing the evaluated fields into the database.


## Mapping

Example mapping from sequencer.py (extract):

    mapper = {
        "users": {
            "usersurname": "'Toto'",
        },
        "recipes": {
            "userID": "session.users.userid",
            "recipeSS": "'Toto'",
            "recipeSSRecipeId": "str(record.id)",
            "recipeDifficulty": "record.difficulty",
            "recipePrepareTime": "translators.convert_duration(record.preparation_time)",
            "recipeSSModifDate": "translators.convert_date(record.modified)",
            "recipeTechDate": "datetime.now()",
        },
        "recipes:check": {
            "userID": "session.users.userid",
            "recipeSS": "'Toto'",
            "recipeSSRecipeId": "str(record.id)",
        },
        "cookingsteps": {
            "cookingstepss": "'Toto'",
            "cookingstepsscookingstepid": "str(step.id)",
            "cookingstepnum": "step.order",
            "cookingStepTechDate": "datetime.now()",
        },
    }

A mapping is a 2D dictionary. First level keys are target database table names. Second level keys are target
tables column names. It is defined in a subclass of `MapperSequencer`.

A single target table can be found more than once in the mapping, as you may need different mappings for
this table. Mappings '1' and '2' for a table 'users' can be identified as 'users:1' and 'users:2'.
Common example is to have a mapping to _select_ a line in a table, and a different mapping to _create_
a new line in this same table.

Values in the mapping are expressions enclosed in strings:
- All expressions will be evaluated during the mapper/sequencer phase.
- Expressions starting with 'session' refer to database records (usually IDs) previously processed.
  A `commit()` or a `flush()` must be performed previously. The expression is in the form `session.table.column`
  or `session['table'].column` if 'table' contains a ":".
- Expressions starting with a record name, like "record.id" or "step.order" above, refer to records
  names as specified in the _sequencer_, see below.
- Expressions starting with 'translators' refer to user-defined conversion functions. See this section below.
- Other expressions are regular Python expressions. You can use constants (like True or False) and any
  built-in or imported function. Imported modules must be specified when you create sequencer instance.
  See _API_ section below.


## Sequencer

Example sequencer from sequencer.py (extract):

    @_records
    def multi_process_records(self, records):
        self.comment('INJECTION FROM MON AUTOCUISEUR')
        if not self.select("users"):
            self.create("users")
            self.commit()
        for record in records:
            self.process_record(record)
            self.commit()

    @_record
    def process_record(self, record):
        if not self.update("recipes:check", "recipes:check_update", "recipes:update"):
            self.create("recipes")
            self.log.info("Create recipe name: " + record.recipe)
            self.flush()
        self.multi_process_steps(record.steps)
        if not self.update("recipeslg:check", "recipeslg:check_update", "recipeslg:update"):
            self.create("recipeslg")

    @_sub_records
    def multi_process_steps(self, steps):
        self.instructions = []
        for step in steps:
            self.process_step(step)

    @_sub_record
    def process_step(self, step):
        if not self.update("cookingsteps:check", "cookingsteps:check_update", "cookingsteps:update"):
            self.create("cookingsteps")
            self.flush()
        self.instructions.append(step.desc)
        if not self.update("cookingstepslg:check", "cookingstepslg:check_update", "cookingstepslg:update"):
            self.create("cookingstepslg")
        if not self.exists("recipecontainscookingsteps"):
            self.create("recipecontainscookingsteps")

Sequencing of import is defined as a set of user-defined methods in a class derived from `MapperSequencer`.
These methods are preceded by Python decorators related to _levels_. There are 4 levels.

The first level (root) method must be preceded by decorator `_records`. This method has a single parameter which
will hold a python list of the records to import into the database. This method will return True on success and
False if an error occurs or the sequencer is aborted. The return value is computed by the decorator.

This method then calls, for each record, a second method which has a single parameter which holds a python
dictionary representing this single record.
The name of this parameter is important as it is retrieved by introspection and must match with
corresponding expressions in the mapping. This second level method must be preceded by decorator `_record`.

If records contain sub-records, this last method should then call, for each list of sub-records, a third
level method which purpose is to loop over each record of the list. This 3rd level method must be preceded
by decorator `_sub_records`.

At last (phew...) this method calls, for each sub-record, a fourth level method that has a single parameter
which name match that of expressions in the mapping. This method definition must be preceded by decorator
`_sub_record`.

The purpose of decorators is to manage sequencer interruption (via special Exception SequencerInterruption)
logging, global variables, return values and other data conversions between levels.

It is mandatory to have a new method each time you have to loop over a sub-record. This comes from the fact
that naming of sub-records is done implicitly by the name of the formal parameter of the method.


## API

The module **dbMapLoader.py** defines 2 main classes `MapperSequencer` for mapping/sequencing and
`Injector` for injection.

Typical usage of `MapperSequencer` is to create your own class derived from it. In this class you will
define your mapping in `mapper` class variable. See _Mapping_ section above.
You will also define your sequencing by writing a set of methods with a root method that starts the whole
mapping/sequencer process. See _Sequencer_ section above.

Class `MapperSequencer` offers a set of methods that you use in your sequencer for checking, skipping or
creating records in your database from your mapping. Other methods allow to flush and
commit records created up to the current step. You can also introduce a comment.

These methods are:
- `create(_table)`	                       creates record with mapping '_table'. Can fail if a unique filed is duplicated.
- `select(_table)`	                       selects record with mapping '_table'. Can fail if multiple records found.
                                           returns True if one record found, False if no record found.
- `exists(_table)`                         returns True if one or more records found. Can not fail.
- `update(_check, _check_update, _update)` performs `select(_check)` and if success, compares fields of selected record
                                           to fields of '_check_update'. If at least one field differ, will update the
                                           whole record with values in '_update'.
- `flush()`     flushes the session. Necessary to set `session` variable from previous `create()` or `update()`.
- `commit()`    commits the session.
- `comment()`   allows to insert a comment into the sequencer flat file.
- `log.<criticity>(message)` allows to insert a message in the log file. 'criticity' has values among: info,
   warning and error.
- `abort()`     allows to abort sequencer (root user-defined method will return False).

Methods `select()`, `create()` and `update()` set the global variable `session` according to the exact value that is present
into the database. Method `exist()` does not set `session`. See _Mapping_ section above.

Method `__init__()` of MapperSequencer has 2 optional parameters:
- a log object (default=1) that defaults to a raw console logger at warning level.
- a tuple of imports specifications. An import specification is a tuple (module_name, symbols).
  if symbols evaluates to False, this import specification will result in `import module_name`.
  if symbols evaluates to an iterable of strings i.e. (symbol1, ..., symboln) this import specification will
  result in `from module_name import symbol1, ..., symboln`.

Class `Injector` offers methods for checking your mapping, inject your records into your database from the
sequencer flat file produced by the mapping/sequencer phase. Other methods allow for introspection of your
database.

These methods are:
- `get_tablenames()`              get table names from the database you have connected to.
- `get_columnames(table)`         get columns names for a given table.
- `check_mapping(mapper, kwargs)` check your mapping against the database you have connected to.
- `prepare_session(mapper)`       prepare injection objects an data from your mapping.
- `prepare_injection(records)`    prepare injection objects an data from your (optional) list of records.

Class `Shell`. This class allows to link an injector instance to a sequencer/mapper instance.

## User-defined conversion functions

Example user-defined conversion from sequencer.py (extract):

    def convert_date(value):
        _date, _time = value.split('T')
        args = list(_date.split('-'))
        args.extend(_time.split(':'))
        return datetime(*[int(x) for x in args])
    translators.set_static(convert_date)

The function is defined as a regular Python function and then injected in the class `translators` as a static
function.

## Definition of a format for data exchange between recipes databases

The format is based on JSON.
A set of recipes is represented as a list of records, each record being a single recipe.

	[
		{ record 1}
		{ record 2}
		…
		{ record n}
	]

A record is a dictionary (key/values) where keys are field names and value types are taken from the (short) list
of JSON permitted types:

	Unicode string,
	Numerical value (float or integer),
	Null,
	List or dictionary.

Such a definition suggests that a recipe can be completely defined into a single (nested / complex) record,
with associated records (like steps, lists of ingredients, comments…) directly specified in the recipe as lists of sub-records.

	{
		Name: “fish and chips”,
		total_time: “1 h”,
		prepare_time: “25 min”,
		difficulty: 1,
		creation_date: “2010 12 24 18 00 00”,
		ingredients: [
		{ ... },
		{ ... },
		…
		],
		Steps: [
		…
		],
		Comments: [
		…
		]
	}

Dates are specified as string fields: “Year month day hour minute second” for example.
Durations can be specified either as string “1 h 15 min”, “20 min” or as an integer: 75 (meaning 1h 15min).


## Getting started

1. Git clone me and get pythor.py from [pythor] (https://github.com/francois-vincent/pythor)

2. Start your database server and create your database. The less required is a database with all mappings
   already present. Edit your connection file with your connection parameters.
   If you do not have a database, you can use option `--sequencer-only`.

3. Edit sequencer.py (i.e. edit your mapping, your sequencer and products). Products is a dictionary
   specifying log and flat files names and prefixes.

4. Get a JSON records file _Records.json_, then launch:

        python sequencer.py Records.json

5. If you only want to check your mapping against the database, launch:

        python sequencer.py Records.json -c

6. If you only want to run the sequencer (no database required), launch:

        python sequencer.py Records.json -s

7. You can inspect log file and flat file from injector in the folders specified in products.

## Tests

A non-regression test scenario is provided in file test/testLoader.py. This file is all-inclusive and only requires
the schema of the database.


## Remote execution

A Python fabric script is provided that allows to run the standalone script directly on the target computer.
This allows to speed up injection process by a factor 5 to 7, especially when the data set to inject is huge.

You can change the remote command line in the data structure `FabContext`.

You can change the connection parameters in the data structure `connection`.

The products of the remote injection (log and flat files) will be downloaded back in local computer, in a directory named
\_host\_\<remote_host_name\>.


## Dependencies

You will have to install SQLAlchemy plus any Python wrapper to your favorite databases adapter (psycopg2
for PostgreSQL, pymysql for MySQL).

Command line parsing (for the standalone usage) requires pythor.py which you can get from my github.

Running no regression tests requires Python module Unittest2.

Running remote script requires Python module fabric.


## Authors

 - François Vincent [mail] (mailto:fvincent@groupeseb.com) - [github] (https://github.com/francois-vincent)
