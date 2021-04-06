'''
Test the managekeys and Keys classes
'''
from quotedb.models.managekeys import Keys, ManageKeys, Session
from quotedb.dbconnection import getSaConn

import unittest
from unittest import TestCase

sqlitedb = 'sqlite:///test_key_db.sqlite'


class TestManageKeys(TestCase):

    testdb = 'sqlite:///test_keys.sqlite'

    def test_ManageKeysAdd_RemoveKey(self):

        mk = ManageKeys(self.testdb, create=True)
        for n, p in zip(['fred', 'wilma', 'pebbles', 'fred'], ['flintstone', 'flinttone', 'flintstone', 'replaced']):
            Keys.addKey(n, p, mk.session)

        s = Session(bind=mk.engine)
        q = s.query(Keys).all()
        for n, p in zip(['wilma', 'pebbles', 'fred'], ['flinttone', 'flintstone', 'replaced']):
            self.assertEqual(Keys.getKey(n, mk.session), p)

        for n in ['wilma', 'pebbles', 'fred']:
            Keys.removeKey(n, mk.session)
        q = s.query(Keys).all()
        self.assertEqual(q, [])

    def test_installDb(self):
        from quotedb.models.managekeys import constr
        mk = ManageKeys(constr)
        Keys.installDb(mk.session, install='dev')
        db = getSaConn(refresh=True)
        self.assertGreater(db.find("/dev_stockdb"), 0)

    def test_installDb_uninstall(self):
        from quotedb.models.managekeys import constr
        mk = ManageKeys(constr)
        Keys.installDb(mk.session, install='prod')
        db = getSaConn(refresh=True)
        self.assertGreater(db.find("/stockdb"), 0)


if __name__ == '__main__':
    unittest.main()
    # tm = TestManageKeys()
    # tm.test_installDb()
    # tm.test_installDb_uninstall()
    # print(getSaConn())
