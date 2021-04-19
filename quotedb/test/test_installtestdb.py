import unittest
from unittest import TestCase

from quotedb.dbconnection import getSaConn
from quotedb.scripts.installtestdb import main as installmain


class MachAhgs:
    production = False
    test = False
    show = False


class TestInstallTestDb(TestCase):
    def test_installTestDb_production(self):
        ahgs = MachAhgs()
        ahgs.test = True
        installmain(ahgs)
        ahgs.test = False
        ahgs.production = True
        installmain(ahgs)
        db = getSaConn()
        self.assertGreater(db.find("localhost/stockdb"), 0)
        self.assertLess(db.find("/dev_stockdb"), 0)

    def test_installTestDb_test(self):
        ahgs = MachAhgs()
        ahgs.production = True
        installmain(ahgs)
        ahgs.production = False
        ahgs.test = True
        installmain(ahgs)
        db = getSaConn()
        self.assertGreater(db.find("/dev_stockdb"), 0)
        self.assertLess(db.find("localhost/stockdb"), 0)

    def test_installTestDb_show(self):
        ahgs = MachAhgs()
        ahgs.show = True
        db1 = getSaConn()
        x = installmain(ahgs)
        db2 = getSaConn()
        self.assertEqual(db1, db2)
        self.assertIsNotNone(x)


if __name__ == '__main__':
    unittest.main()
