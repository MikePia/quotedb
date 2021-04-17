'''
Test the managekeys and Keys classes
'''
from quotedb.models.managekeys import Keys, ManageKeys, Session, cleanup_sqlite, init_sqlite, getEngine
from quotedb.dbconnection import getSaConn
from quotedb.models.managekeys import constr

import unittest
from unittest import TestCase

sqlitedb = 'sqlite:///test_key_db.sqlite'


class TestManageKeys(TestCase):

    testdb = 'sqlite:///test_keys.sqlite'

    @classmethod
    def setUpClass(cls):
        _ = ManageKeys(sqlitedb)
        statement = f'delete from {Keys.__tablename__}'
        getEngine().execute(statement)
        cleanup_sqlite()

    def test_AllKeys(self):
        # Test all keys in the main sqlite db are present and have a value
        keys = ["fh_token", "poly_token", "mysql_ip", "mysql_port", "mysql_user", "mysql_pw",
                "mysql_db", "mysql_db_bak", "mysql_user_bak", "mysql_pw_bak",
                "mysql_db_dev", "mysql_user_dev", "mysql_pw_dev"]
        mk = ManageKeys(constr)
        for key in keys:
            value = Keys.getKey(key, mk.session)
            self.assertIsNotNone(value, f"The local config is not setup correctly for the key {key}")

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
        cleanup_sqlite()
        init_sqlite()
        q = s.query(Keys).all()
        self.assertEqual(q, [])

    def test_installDb(self):
        cleanup_sqlite()
        init_sqlite()
        Keys.installDb(install='dev')
        db = getSaConn(refresh=True)
        print(db)
        self.assertGreater(db.find("/dev_stockdb"), 0)

    def test_installDb_uninstall(self):
        cleanup_sqlite()
        init_sqlite()
        Keys.installDb(install='production')
        db = getSaConn(refresh=True)
        print(db)
        self.assertGreater(db.find("/stockdb"), 0)


if __name__ == '__main__':
    unittest.main()
    # tm = TestManageKeys()
    # tmq.test_ManageKeysAdd_RemoveKey()
    # tm.test_AllKeys()
    # tm.test_installDb()
    # tm.test_installDb_uninstall()
    # print(getSaConn())
