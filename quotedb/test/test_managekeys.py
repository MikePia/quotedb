'''
Test the managekeys and Keys classes
'''
from quotedb.models.managekeys import Keys, ManageKeys, Base, Session

import unittest
from unittest import TestCase

sqlitedb = 'sqlite:///test_key_db.sqlite'


class TestManageKeys(TestCase):

    testdb = 'sqlite:///test_keys.sqlite'


    def test_ManageKeysAdd_RemoveKey(self):

        mk = ManageKeys(self.testdb, create=True)
        for n,p in  zip (['fred', 'wilma', 'pebbles', 'fred'], ['flintstone', 'flinttone', 'flintstone', 'replaced']):
            Keys.addKey(n, p, mk.engine)

        s = Session(bind=mk.engine)
        q = s.query(Keys).all()
        for n, p in  zip (['wilma', 'pebbles', 'fred'], ['flinttone', 'flintstone', 'replaced']):
            self.assertEqual(Keys.getKey(n, mk.engine), p)

        for n in  ['wilma', 'pebbles', 'fred']:
            Keys.removeKey(n, mk.engine)
        q = s.query(Keys).all()
        self.assertEqual(q, [])
        

if __name__ == '__main__':
    unittest.main()


