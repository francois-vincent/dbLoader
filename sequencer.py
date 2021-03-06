# -*- coding: utf-8 -*-

__version__ = '0.3.1'
__date__ = 'dec 18th, 2012'
__author__ = 'François Vincent'
__mail__ = 'fvincent@groupeseb.com'
__github__ = 'https://github.com/francois-vincent'

"""
This is an example sequencer and mapper
"""

import os, time, sys
import files.simpleLogger as logger
from dbMapLoader import MapperSequencer, _records, _record, _sub_records, _sub_record, Shell
from dbMapLoader import Injector, translators
from datetime import datetime
from files.json_pp import json_dump, json_load

# WARNING: it appears that date conversion from format YYYY-MM-DDTHH:MM:SS is automatic
# so one may be tempted not to use the conversion function above.
# if you do so, the update method will fail to consider dates as equal (because a
# string is never equal to a date) and undue updates will silently take place !
def convert_datetime(value):
    _date, _time = value.split('T')
    args = list(_date.split('-'))
    args.extend(_time.split(':'))
    return datetime(*[int(x) for x in args])
translators.set_static(convert_datetime)

class myMapperSequencer(MapperSequencer):
    mapper = {
        "users": {
            "usersurname": "'MonAutocuiseur'",
        },
        "recipes": {
            "userID": "session.users.userid",
            "recipeSS": "'MonAutocuiseur'",
            "recipeSSRecipeId": "str(record.id)",
            "recipeYield": "record.yield_value",
            "recipePrepareTime": "translators.convert_duration(record.preparation_time)",
            "recipeCookTime": "translators.convert_duration(record.cooking_time)",
            "recipeTotalTime": "translators.convert_duration(record.total_time)",
            "recipePublicationDate": "datetime.now()",
            "recipeSSModifDate": "translators.convert_date(record.modified)",
            "recipeTechDate": "datetime.now()",
        },
        "recipes:check": {
            "userID": "session.users.userid",
            "recipeSS": "'MonAutocuiseur'",
            "recipeSSRecipeId": "str(record.id)",
        },
        "recipes:check_update": {
            "recipeYield": "record.yield_value",
            "recipePrepareTime": "translators.convert_duration(record.preparation_time)",
            "recipeCookTime": "translators.convert_duration(record.cooking_time)",
            "recipeTotalTime": "translators.convert_duration(record.total_time)",
            "recipeSSModifDate": "translators.convert_date(record.modified)",
        },
        "recipes:update": {
            "recipeYield": "record.yield_value",
            "recipePrepareTime": "translators.convert_duration(record.preparation_time)",
            "recipeCookTime": "translators.convert_duration(record.cooking_time)",
            "recipeTotalTime": "translators.convert_duration(record.total_time)",
            "recipeSSModifDate": "translators.convert_date(record.modified)",
            "recipeTechDate": "datetime.now()",
        },
        "recipeslg": {
            "recipeid": "session.recipes.recipeid",
            "recipeLGName": "record.recipe",
            "recipelgidlang": "'fr'",
            "recipelgsummary": "record.summary",
            "recipelgingredientssummary": "' - '.join(record.ingredients_summary)",
            "recipelgInstructions": "' - '.join(self.instructions)",
            "RecipeLGTechDate": "datetime.now()",
        },
        "recipeslg:check": {
            "recipeid": "session.recipes.recipeid",
            "recipelgidlang": "'fr'",
        },
        "recipeslg:check_update": {
            "recipeLGName": "record.recipe",
            "recipelgsummary": "record.summary",
            "recipelgingredientssummary": "' - '.join(record.ingredients_summary)",
            "recipelgInstructions": "' - '.join(self.instructions)",
        },
        "recipeslg:update": {
            "recipeLGName": "record.recipe",
            "recipelgsummary": "record.summary",
            "recipelgingredientssummary": "' - '.join(record.ingredients_summary)",
            "recipelgInstructions": "' - '.join(self.instructions)",
            "RecipeLGTechDate": "datetime.now()",
        },
        "cookingsteps": {
            "cookingstepnum": "step.order",
            "cookingstepss": "'MonAutocuiseur'",
            "cookingstepsscookingstepid": "str(step.id)",
            "cookingStepTechDate": "datetime.now()",
        },
        "cookingsteps:check": {
            "cookingstepss": "'MonAutocuiseur'",
            "cookingstepsscookingstepid": "str(step.id)",
        },
        "cookingsteps:check_update": {
            "cookingstepnum": "step.order",
        },
        "cookingsteps:update": {
            "cookingstepnum": "step.order",
            "cookingStepTechDate": "datetime.now()",
        },
        "cookingstepslg": {
            "cookingstepid": "session.cookingsteps.cookingstepid",
            "cookingsteplgidlang": "'fr'",
            "cookingsteplgname": "step.desc",
            "cookingStepLGTechDate": "datetime.now()",
        },
        "cookingstepslg:check": {
            "cookingstepid": "session.cookingsteps.cookingstepid",
            "cookingsteplgidlang": "'fr'",
        },
        "cookingstepslg:check_update": {
            "cookingsteplgname": "step.desc",
        },
        "cookingstepslg:update": {
            "cookingsteplgname": "step.desc",
            "cookingStepLGTechDate": "datetime.now()",
        },
        "recipecontainscookingsteps": {
            "recipeid": "session.recipes.recipeid",
            "cookingstepid": "session.cookingsteps.cookingstepid",
        },
    }

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

#      Utility functions
# --------------------------

# TODO rework files accesses

def check_prefixes(prefixes):
    for p in set(prefixes):
        check_prefix(p)

def check_prefix(prefix):
    "check folder write access and eventually set path"
    if not os.path.exists(prefix):
        try:
            os.makedirs(prefix)
        except:
            raise RuntimeError("Can't set folder <%s>" % prefix)
    if not os.access(prefix, os.W_OK):
        raise RuntimeError("Folder <%s> is not writable" % prefix)

def check_file_access(master, name, required=True, ext='', create=False):
    """
    check a file specification in master, with eventual prefix
    and optional creation
    """
    split_name = name.split('.')
    father = master
    last_name = split_name[-1]
    for x in split_name[:-1]:
        father = father[x]
    if required:
        result = father[last_name]
    else:
        result = father.get(last_name, None)
    if result:
        pref = master['prefixes'].get(name, None)
        if pref:
            result = os.path.join(pref, result)
        if create:
            result += ext
            f = open(result, 'w')
            f.close()
        elif not os.access(result + ext, os.R_OK):
            raise RuntimeError('%s file not found <%s>' % (name.capitalize(), result + ext))
    return result


products = {
    "prefixes": {
        "log": "log",
        "flat_seq": "flat_seq",
        "flat_inj": "flat_inj"
    },
    "log": "",
    "flat_seq": "flat_seq",
    "flat_inj": "flat_inj"
}

#     Main Function
# ----------------------
def inject_from_master(records_json, database_connection, check_only, no_check, sequencer_only, _eval, abort_on_error):
    """
    This is the main function that creates datastructures from files, controls main classes instances and saves products
    record_json is a file containing a list of records.
    database_connection is a file containing connection parameters.
    """
    check_prefixes(products['prefixes'].values())
    # set logging
    log = products.get('log', None)
    logging = logger.getLogger(filename=log, prefix=products['prefixes'].get('log', None), screen=logger.WARNING,
                               color=True)
    with logging as log:
        log.info(
            "Launched %s records_json=%s, database_connection=%s. Options: check_only=%s, no_check=%s, sequencer_only=%s" %\
            (sys.argv[0], records_json, database_connection, check_only, no_check, sequencer_only))
        if sequencer_only:
            no_check = True
        if no_check and check_only:
            raise RuntimeError("Incompatible options <check_only> and <no_check>")
            # check files access
        if not os.access(records_json, os.R_OK):
            raise RuntimeError('JSON records file <%s> not found' % records_json)
        if not os.access(database_connection, os.R_OK):
            raise RuntimeError('Connection file <%s> not found' % database_connection)
        flat_seq = check_file_access(products, 'flat_seq', ext=time.strftime('_%Y-%m-%d_%H-%M-%S.json'), required=False,
                                     create=True)
        if not sequencer_only:
            flat_inj = check_file_access(products, 'flat_inj', ext=time.strftime('_%Y-%m-%d_%H-%M-%S.json'),
                                         required=False, create=True)
            connection = json_load(database_connection)
        # load records, mapper and sequencer and launch injection
        if _eval:
            records = json_load(records_json)
        else:
            records = json_load(records_json, 'json')
        if not (isinstance(records, list) and isinstance(records[0], dict)):
            raise RuntimeError('JSON records file <%s> should be a list of dictionaries' % records_json)
        if sequencer_only:
            injector = None
            no_check = True
        else:
            dsn = connection['dsn'] % connection['connection_context']
            kwargs = dict(records_json=records_json, connection=connection)
            injector = Injector(dsn, kwargs, log)
        mapseq = myMapperSequencer(log=log, imports=(('datetime', ('datetime',)),), aoe=abort_on_error)
        if no_check or injector.check_mapping(mapseq.mapper):
            if not check_only:
                if injector:
                    injector.prepare_session(myMapperSequencer.mapper).prepare_injection()
                    Shell(mapseq, injector)
                result = mapseq.multi_process_records(records)
                json_dump(mapseq.flat, flat_seq)
                if not sequencer_only:
                    json_dump(injector.flat, flat_inj)


if __name__ == '__main__':
    # this is the command line frontend
    # module pythor can be found on my github
    from  pythor import cmdLineExtract

    class CommandLine(cmdLineExtract):
        """
        This application will help you inject a set of compound records into any kind of relational database supported by SQLAlchemy.
        """
        options_aliases = cmdLineExtract.autoShort
        def run(self, records_json, database='connection.json', check_only=bool(), no_check=bool(), sequencer_only=bool(), eval=bool(), abort_on_error=False):
            inject_from_master(records_json, database, check_only, no_check, sequencer_only, eval, abort_on_error)

    CommandLine().start()
