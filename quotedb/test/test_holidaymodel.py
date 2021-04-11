import datetime as dt
import logging
import os
from unittest import TestCase
import unittest

from quotedb.dbconnection import getSaConn
from quotedb.models.holidaymodel import ManageHolidayModel, HolidayModel
from quotedb.models.metamod import init, getSession
from quotedb.scripts.installtestdb import installTestDb


class TestHolidayModel(TestCase):
    stocks = None
    start = None
    fn = None

    @classmethod
    def setUpClass(cls):
        print("\nTestHolidayModel.setUpClass()")
        installTestDb(install="dev")
        db = getSaConn(refresh=True)
        # Prevent tests from running if not using dev_quotedb
        assert db.find("dev_stockdb") > 0
        print('Set the database to the test database dev_stockdb')
        init()
        ddiirr = os.path.dirname(__file__)
        fn = os.path.join(ddiirr, "../models/holidays.csv")
        if not os.path.exists(fn):
            # Note, if the file is missing but the db is already setup. Only the creation test will fail.
            logging.error("TestHolidayModel is missing the file holidays.csv. Some tests will fail")
            return
        cls.fn = os.path.abspath(fn)

        mh = ManageHolidayModel(create=True)

        mh.saveHolidays(cls.fn)

    @classmethod
    def tearDownClass(cls):
        print("\nTestHolidayModel.tearDownClass()")
        installTestDb(install="production")
        db = getSaConn(refresh=True)
        assert db.find("dev_stockdb") < 0
        print('Reset db to stockdb')

    def test_isHoliday(self):
        s = getSession()
        holidays = [
            dt.datetime(2019, 1, 1), dt.datetime(2020, 1, 1), dt.datetime(2021, 1, 1),
            dt.datetime(2019, 12, 25), dt.datetime(2020, 12, 25),
            dt.datetime(2021, 12, 24)]  # Observed on Friday, 24 in 2021
        notholidays = [
            dt.datetime(2019, 1, 2), dt.datetime(2020, 1, 2), dt.datetime(2021, 1, 2),
            dt.datetime(2019, 12, 26), dt.datetime(2020, 12, 26), dt.datetime(2021, 12, 26)]

        for d1, d2 in zip(holidays, notholidays):
            self.assertTrue(HolidayModel.isHoliday(d1, s))
            self.assertFalse(HolidayModel.isHoliday(d2, s))


if __name__ == '__main__':
    unittest.main()
