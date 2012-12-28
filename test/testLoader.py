# -*- coding: utf-8 -*-

__version__ = '0.1.0'
__date__ = 'dec 27th, 2012'
__author__ = 'François Vincent'
__mail__ = 'fvincent@groupeseb.com'
__github__ = 'https://github.com/francois-vincent'

import unittest2 as unittest
import subprocess
import sys, os
from cStringIO import StringIO

test_database_name = 'testbase_loader'
psql_fixture = 'create_dbanalytic.sql'

connection = {
    "connection_context" : {
        "name":   test_database_name,
        "user":   "postgres",
        "passwd": "postgres",
        "type":   "postgresql",
        "host":   "127.0.0.1",
        "port":   "5432"
    },
    "dsn" : "%(type)s://%(user)s:%(passwd)s@%(host)s:%(port)s/%(name)s",
    "schema" : "sch_dbanalytic",
}

records_json = [
  {
    "total_time": 54,
    "nutritions": [],
    "ingredients_summary": [
      "1 kg de poires m\u00fbres",
      "50 g de groseilles \u00e9grapp\u00e9es",
      "50 g de sucre",
      "4 feuilles de g\u00e9latine",
      "10 cl de cr\u00e8me liquide",
      "2 cuill\u00e8res \u00e0 soupe d'alcool de poire",
      "10 cl d'eau"
    ],
    "tags": [],
    "preparation_time": 45,
    "created": "2012-09-05T19:13:26",
    "recipe": "Bavaroise aux poires et aux groseilles",
    "modified": "2012-12-19T16:39:27",
    "summary": "",
    "steps": [
      {
        "modified_by": "/v1/users/42/",
        "created": "2012-09-05T19:13:28",
        "modified": "2012-09-05T19:13:28",
        "created_by": "/v1/users/42/",
        "id": 2047,
        "order": 0,
        "desc": "Pelez les poires, coupez-les en deux. Retirez le coeur."
      },
      {
        "modified_by": "/v1/users/42/",
        "created": "2012-09-05T19:13:28",
        "modified": "2012-09-05T19:13:28",
        "created_by": "/v1/users/42/",
        "id": 2048,
        "order": 1,
        "desc": "Dans l'autocuiseur, mettez les poires, le sucre et l'eau. Fermez l'autocuiseur."
      },
      {
        "modified_by": "/v1/users/42/",
        "created": "2012-09-05T19:13:29",
        "modified": "2012-09-05T19:13:29",
        "created_by": "/v1/users/42/",
        "id": 2049,
        "order": 2,
        "desc": " D\u00e8s que la vapeur s'\u00e9chappe, baissez le feu et laissez cuire selon le temps indiqu\u00e9."
      },
      {
        "modified_by": "/v1/users/42/",
        "created": "2012-09-05T19:13:29",
        "modified": "2012-09-05T19:13:29",
        "created_by": "/v1/users/42/",
        "id": 2050,
        "order": 3,
        "desc": "Ouvrez l'autocuiseur et \u00e9gouttez les poires. Laissez-les refroidir. Fouettez la cr\u00e8me. Passez les feuilles de g\u00e9latine sous l'eau. Faites fondre la g\u00e9latine lav\u00e9e dans 10 cl du jus de cuisson des poires encore chaud. Mixez 800 gr de poires, incorporez le sirop avec la g\u00e9latine puis la cr\u00e8me fouett\u00e9e. Emincez le reste des poires."
      },
      {
        "modified_by": "/v1/users/42/",
        "created": "2012-09-05T19:13:29",
        "modified": "2012-09-05T19:13:29",
        "created_by": "/v1/users/42/",
        "id": 2051,
        "order": 4,
        "desc": "Versez dans un moule \u00e0 cake une partie de la pur\u00e9e de poires. Disposez une couche de poires \u00e9minc\u00e9es et des groseilles. Puis ajoutez une couche de pur\u00e9e de poires. Renouvelez l'op\u00e9ration plusieurs fois. Terminez par une couche de pur\u00e9e de poires. Laissez prendre au r\u00e9frig\u00e9rateur pendant 12 heures. "
      },
      {
        "modified_by": "/v1/users/42/",
        "created": "2012-09-05T19:13:29",
        "modified": "2012-09-05T19:13:29",
        "created_by": "/v1/users/42/",
        "id": 2052,
        "order": 5,
        "desc": "D\u00e9moulez, d\u00e9corez avec de la cr\u00e8me chantilly et des groseilles. Servez avec un coulis de fruits rouges."
      }
    ],
    "cooking_time": 9,
    "yield_value": "4.0",
    "category": None,
    "id": 183,
    "yield_unit": "persons"
  }
]

sys.path.insert(0, os.path.dirname(os.getcwd()))
from dbMapLoader import Shell, Injector
from sequencer import myMapperSequencer

class testSequencer(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        unittest.TestCase.__init__(self, *args, **kwargs)
    @classmethod
    def setUpClass(cls):
        print "Creating a new empty test database..."
        null = open('/dev/null', 'w')
        create_cmd = 'createdb %s -U postgres' % test_database_name
        drop_cmd = 'dropdb %s -U postgres' % test_database_name
        if subprocess.call(create_cmd, shell=True, stderr=null):
            subprocess.call(drop_cmd, shell=True)
            subprocess.call(create_cmd, shell=True)
        p = subprocess.Popen('psql %s -U postgres -f %s' % (test_database_name, psql_fixture), shell=True,
                             stdout=null, stderr=subprocess.PIPE)
        errcode = p.wait()
        if errcode or 'ERROR' in p.stderr.read():
            raise RuntimeError('database creation error')
    @classmethod
    def tearDownClass(cls):
        pass
    def setUp(self):
        self.connect()
    def tearDown(self):
        sys.stderr = sys.__stderr__
    # ---------- Helper methods --------------------------------------------------
    def connect(self):
        dsn = connection['dsn'] % connection['connection_context']
        self.kwargs = dict(records_json=records_json, connection=connection)
        self.injector = Injector(dsn, self.kwargs, log=0)
    def get_tables(self):
        query = 'select * from sch_dbanalytic.users'
        self.__class__.users = self.injector.engine.execute(query).fetchall()
        query = 'select * from sch_dbanalytic.recipes'
        self.__class__.recipes = self.injector.engine.execute(query).fetchall()
        query = 'select * from sch_dbanalytic.recipeslg'
        self.__class__.recipeslg = self.injector.engine.execute(query).fetchall()
        query = 'select * from sch_dbanalytic.cookingsteps'
        self.__class__.cookingsteps = self.injector.engine.execute(query).fetchall()
        query = 'select * from sch_dbanalytic.cookingstepslg'
        self.__class__.cookingstepslg = self.injector.engine.execute(query).fetchall()
    # ---------- Test methods start here ---------------------------------------------
    def test_mapping(self):
        # a good mapping passes through check_mapping
        self.assertTrue(self.injector.check_mapping(myMapperSequencer.mapper))
    def test_bad_mapping(self):
        # a bad mapping with a bad table does not pass through check_mapping. Check error message too
        self.bad_mapping = {'toto': {}}
        sys.stderr = StringIO()
        self.assertFalse(self.injector.check_mapping(self.bad_mapping))
        self.assertIn('Table <toto> not in', sys.stderr.getvalue())
    def test_bad_column(self):
        # a bad mapping with a bad column does not pass through check_mapping. Check error message too
        self.bad_mapping = { "cookingstepslg": {
            "cookingStepLGTechDatex": "",
        } }
        sys.stderr = StringIO()
        self.assertFalse(self.injector.check_mapping(self.bad_mapping))
        self.assertIn('Column <cookingStepLGTechDatex> not in', sys.stderr.getvalue())
    def test_first_injection(self):
        # check injection of a single recipe with cooking steps, in an empty database
        self.mapseq = myMapperSequencer(log=0, imports=(('datetime', ('datetime',)),))
        self.injector.prepare_session(self.mapseq.mapper).prepare_injection(records_json)
        Shell(self.mapseq, self.injector)
        self.assertTrue(self.mapseq.multi_process_records(records_json))
        self.assertEqual(len(self.injector.flat), 21)
        self.get_tables()
        self.assertEqual(len(self.users), 1)
        self.assertEqual(len(self.recipes), 1)
        self.assertEqual(len(self.recipeslg), 1)
        self.assertEqual(len(self.cookingsteps), 6)
        self.assertEqual(len(self.cookingstepslg), 6)
    def test_2nd_injection(self):
        # check injection of the same recipe just after, should change nothing
        self.mapseq = myMapperSequencer(log=0, imports=(('datetime', ('datetime',)),))
        self.injector.prepare_session(self.mapseq.mapper).prepare_injection(records_json)
        Shell(self.mapseq, self.injector)
        self.assertTrue(self.mapseq.multi_process_records(records_json))
        self.assertEqual(len(self.injector.flat), 1)
        users, recipes, recipeslg = self.users, self.recipes, self.recipeslg
        cookingsteps, cookingstepslg = self.cookingsteps, self.cookingstepslg
        self.get_tables()
        self.assertListEqual(users, self.users)
        self.assertListEqual(recipes, self.recipes)
        self.assertListEqual(recipeslg, self.recipeslg)
        self.assertListEqual(cookingsteps, self.cookingsteps)
        self.assertListEqual(cookingstepslg, self.cookingstepslg)
    def test_modified_injection(self):
        # change one cooking step description. should change nothing except this cookingsteplg
        # and recipeslg.recipelgInstructions
        self.mapseq = myMapperSequencer(log=0, imports=(('datetime', ('datetime',)),))
        new_desc = u"Pelez les poires, coupez-les en quatre. Retirez le coeur."
        records_json[0]['steps'][0]['desc'] = new_desc
        self.injector.prepare_session(self.mapseq.mapper).prepare_injection(records_json)
        Shell(self.mapseq, self.injector)
        self.assertTrue(self.mapseq.multi_process_records(records_json))
        self.assertEqual(len(self.injector.flat), 3)
        users, recipes, recipeslg = self.users, self.recipes, self.recipeslg
        cookingsteps, cookingstepslg = self.cookingsteps, self.cookingstepslg
        self.get_tables()
        self.assertListEqual(users, self.users)
        self.assertListEqual(recipes, self.recipes)
        self.assertListEqual(cookingsteps, self.cookingsteps)
        self.assertListEqual(sorted(cookingstepslg, key=lambda x:x[0])[1:], sorted(self.cookingstepslg, key=lambda x:x[0])[1:])
        query = 'select cookingsteplgname from sch_dbanalytic.cookingstepslg where cookingstepid=1'
        self.assertEqual(self.injector.engine.execute(query).fetchall()[0][0], new_desc)
        self.assertEqual(len(self.recipeslg), 1)
        recipelg1 = list(recipeslg[0])
        recipelg2 = list(self.recipeslg[0])
        recipelgInstructions1 = recipelg1[4]
        recipelgInstructions2 = recipelg2[4]
        self.assertNotEqual(recipelgInstructions1, recipelgInstructions2)
        self.assertListEqual(recipelg1[0:4]+recipelg1[5:-1],recipelg2[0:4]+recipelg2[5:-1])
    def test_modified_id_injection(self):
        # change id of recipe. should add a recipe, recipelg plus links
        self.mapseq = myMapperSequencer(log=0, imports=(('datetime', ('datetime',)),))
        records_json[0]['id'] = 118833
        self.injector.prepare_session(self.mapseq.mapper).prepare_injection(records_json)
        Shell(self.mapseq, self.injector)
        self.assertTrue(self.mapseq.multi_process_records(records_json))
        self.assertEqual(len(self.injector.flat), 9)
        query = 'select * from sch_dbanalytic.recipes'
        self.assertEqual(len(self.injector.engine.execute(query).fetchall()), 2)
        query = 'select * from sch_dbanalytic.recipeslg'
        self.assertEqual(len(self.injector.engine.execute(query).fetchall()), 2)

if __name__ == '__main__':
    # this is a test scenario, thus order is relevant
    suite = unittest.TestSuite()
    for test_name in ['test_mapping', 'test_bad_mapping', 'test_bad_column', 'test_first_injection',
                      'test_2nd_injection', 'test_modified_injection', 'test_modified_id_injection']:
        suite.addTest(testSequencer(test_name))
    unittest.TextTestRunner(verbosity=2).run(suite)
