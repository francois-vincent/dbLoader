# -*- coding: utf-8 -*-

__version__ = '0.1.0'
__date__ = 'dec 27th, 2012'
__author__ = 'François Vincent'
__mail__ = 'fvincent@groupeseb.com'
__github__ = 'https://github.com/francois-vincent'

import unittest2 as unittest
import subprocess

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

import sys, os
from cStringIO import StringIO
sys.path.insert(0, os.path.dirname(os.getcwd()))
from dbMapLoader import MapperSequencer, Shell, Injector
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
    def connect(self):
        dsn = connection['dsn'] % connection['connection_context']
        self.kwargs = dict(records_json=records_json, connection=connection)
        self.injector = Injector(dsn, self.kwargs, log=0)
    def test_mapping(self):
        self.assertTrue(self.injector.check_mapping(myMapperSequencer.mapper))
    def test_bad_mapping(self):
        self.bad_mapping = {'toto': {}}
        sys.stderr = StringIO()
        self.assertFalse(self.injector.check_mapping(self.bad_mapping))
        self.assertIn('Table <toto> not in', sys.stderr.getvalue())
    def test_bad_column(self):
        self.bad_mapping = { "cookingstepslg": {
            "cookingStepLGTechDatex": "",
        } }
        sys.stderr = StringIO()
        self.assertFalse(self.injector.check_mapping(self.bad_mapping))
        self.assertIn('Column <cookingStepLGTechDatex> not in', sys.stderr.getvalue())
    def test_first_injection(self):
        self.mapseq = myMapperSequencer(log=0, imports=(('datetime', ('datetime',)),))
        self.injector.prepare_session(self.mapseq.mapper).prepare_injection(records_json)
        Shell(self.mapseq, self.injector)
        self.assertTrue(self.mapseq.multi_process_records(records_json))
        self.assertEqual(len(self.injector.flat), 21)
        self.assertEqual(sum(x[0] for x in self.injector.engine.execute('select cookingstepnum from sch_dbanalytic.cookingsteps').fetchall()), 15)
    def test_2nd_injection(self):
        self.mapseq = myMapperSequencer(log=0, imports=(('datetime', ('datetime',)),))
        self.injector.prepare_session(self.mapseq.mapper).prepare_injection(records_json)
        Shell(self.mapseq, self.injector)
        self.assertTrue(self.mapseq.multi_process_records(records_json))
        self.assertEqual(len(self.injector.flat), 1)
        self.assertEqual(len(self.injector.engine.execute('select * from sch_dbanalytic.users').fetchall()), 1)
        self.assertEqual(len(self.injector.engine.execute('select * from sch_dbanalytic.recipes').fetchall()), 1)
        self.assertEqual(len(self.injector.engine.execute('select * from sch_dbanalytic.cookingsteps').fetchall()), 6)
        self.assertEqual(len(self.injector.engine.execute('select * from sch_dbanalytic.cookingstepslg').fetchall()), 6)
        self.assertEqual(sum(x[0] for x in self.injector.engine.execute('select cookingstepnum from sch_dbanalytic.cookingsteps').fetchall()), 15)
    def test_modified_injection(self):
        self.mapseq = myMapperSequencer(log=0, imports=(('datetime', ('datetime',)),))
        new_desc = u"Pelez les poires, coupez-les en quatre. Retirez le coeur."
        records_json[0]['steps'][0]['desc'] = new_desc
        self.injector.prepare_session(self.mapseq.mapper).prepare_injection(records_json)
        Shell(self.mapseq, self.injector)
        self.assertTrue(self.mapseq.multi_process_records(records_json))
        self.assertEqual(len(self.injector.flat), 3)
        self.assertEqual(self.injector.engine.execute('select cookingsteplgname from sch_dbanalytic.cookingstepslg where cookingstepid=1').fetchall()[0][0], new_desc)
    def test_modified_id_injection(self):
        self.mapseq = myMapperSequencer(log=0, imports=(('datetime', ('datetime',)),))
        records_json[0]['id'] = 118833
        self.injector.prepare_session(self.mapseq.mapper).prepare_injection(records_json)
        Shell(self.mapseq, self.injector)
        self.assertTrue(self.mapseq.multi_process_records(records_json))
        self.assertEqual(len(self.injector.flat), 9)
        self.assertEqual(len(self.injector.engine.execute('select * from sch_dbanalytic.recipeslg').fetchall()), 2)

if __name__ == '__main__':
    # this is a test scenario, thus order is relevant
    suite = unittest.TestSuite()
    for test_name in ['test_mapping', 'test_bad_mapping', 'test_bad_column', 'test_first_injection',
                      'test_2nd_injection', 'test_modified_injection', 'test_modified_id_injection']:
        suite.addTest(testSequencer(test_name))
    unittest.TextTestRunner(verbosity=2).run(suite)
