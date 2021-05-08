'''
Test the managekeys and Keys classes
'''
import os
from quotedb.models.managekeys import Keys, ManageKeys
from quotedb.dbconnection import getSaConn

import unittest
from unittest import TestCase

sqlitedb = 'sqlite:///test_key_db.sqlite'


class TestManageKeys(TestCase):

    testdb = 'sqlite:///test_keys.sqlite'

    def test_AllKeys(self):
        # Test all keys in the main sqlite db are present and have a value
        keys = ["fh_token", "poly_token", "mysql_ip", "mysql_port", "mysql_user", "mysql_pw",
                "mysql_db", "mysql_db_dev", "mysql_user_dev", "mysql_pw_dev"]
        mk = ManageKeys()
        for key in keys:
            value = Keys.getKey(key, mk.session)
            self.assertIsNotNone(value, f"The local config is not setup correctly for the key {key}")

    def test_installDb(self):
        """
        Test if we get the proper database name production/dev databases.
        To correctly test, bypass the ManageKeys class to get the unaltered environment entries
        """
        Keys.installDb(install='dev')
        db = getSaConn()
        self.assertGreater(db.find("/" + os.environ.get('DB_NAME_DEV')), 0)
        Keys.installDb(install='production')
        db = getSaConn()
        self.assertLess(db.find("/" + os.environ.get('DB_NAME_DEV')), 0)

    def test_installDb_uninstall(self):
        Keys.installDb(install='production')
        db = getSaConn()
        self.assertGreater(db.find("/" + os.environ.get('DB_NAME')), 0)


if __name__ == '__main__':
    unittest.main()
    # tm = TestManageKeys()
    # tm.test_AllKeys()
    # tm.test_installDb()
    # tm.test_installDb_uninstall()
    # print(getSaConn())
