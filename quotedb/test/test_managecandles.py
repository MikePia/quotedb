"""

"""
# import logging
from unittest import TestCase
import unittest

from quotedb.dbconnection import getSaConn
from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.metamod import init, getEngine, cleanup
from quotedb.scripts.installtestdb import installTestDb


def dblcheckDbMode(db=None):
    """In case this is called without setUpClass(), and the db is not in test mode,fail with AssertionError """
    db = db if db else getSaConn()
    assert db.find("dev_stockdb") > 0


class TestManageCandles(TestCase):
    stocks = None
    start = None
    fn = None

    @classmethod
    def setUpClass(cls):
        print("\nTestManageCandles.setUpClass()")
        installTestDb(install="dev")
        cleanup()
        init()
        db = getSaConn(refresh=True)
        # Prevent tests from running if not using dev_quotedb
        dblcheckDbMode(db)
        init()

        tables = ['candles', 'allquotes']
        engine = getEngine()
        for table in tables:
            statement = f"DELETE FROM {table}"
            engine.execute(statement)

    @classmethod
    def tearDownClass(cls):
        print("TestManageCandles.tearDownClass()")
        installTestDb(install="production")
        db = getSaConn(refresh=True)
        dblcheckDbMode(db)

    def test_createTables(self):
        print('TestManageCandles.test_createTables')
        dblcheckDbMode()

        # Delete and restore tables
        tables = ["candles", "allquotes"]
        for table in tables:
            statement = f'drop table if exists {table}'
            getEngine().execute(statement)

        # Need to be imported. And this just prevents a linter error in the IDE
        _ = CandlesModel
        _ = AllquotesModel
        cleanup()
        tabnames = getEngine().table_names()
        for table in tables:
            self.assertNotIn(table, tabnames)
        init()
        tabnames = getEngine().table_names()
        for table in tables:
            self.assertIn(table, tabnames)

    @unittest.skip("getReport() is broken and no pressing need")
    def test_reportShape_candles(self):
        pass
        # print('TestManageCandles.test_reportShape')

    @unittest.skip("getReport() is broken and no pressing need")
    def test_chooseFromReport(self):
        pass
        # print('TestManageCandles.test_chooseFromReport')

    @classmethod
    def test_getLargestTimeGaps(cls):
        print('TestManageCandles.getLargestTimeGaps')

    @classmethod
    def test_getFilledData(cls):
        print('TestManageCandles.test_getFilledData')

    @classmethod
    def test_getFilledDataDays(cls):
        print('TestManageCandles.test_getFilledDataDays')

    @classmethod
    def test_getMaxTimeForEachTicker(cls):
        print('TestManageCandles.test_getMaxTimeForEachTicker')

    @classmethod
    def test_filterGanersLosers(cls):
        print('TestManageCandles.test_filterGanersLosers')

    @classmethod
    def test_addCandles(cls):
        print('TestManageCandles.test_addCandles')

    @classmethod
    def test_getTimeRange(cls):
        print('TestManageCandles.test_getTimeRange')

    @classmethod
    def test_getTimeRangeMultiple(cls):
        print('TestManageCandles.test_getTimeRangeMultiple')

    @classmethod
    def test_getTimeRangeMultipleVpts(cls):
        print('TestManageCandles.test_getTimeRangeMultipleVpts')

    @classmethod
    def test_getTimeRangePlus(cls):
        print('TestManageCandles.test_getTimeRangePlus')

    @classmethod
    def test_getMaxTime(cls):
        print('TestManageCandles.test_getMaxTime')

    @classmethod
    def test_getMinTime(cls):
        print('TestManageCandles.test_getMinTime')

    @classmethod
    def test_getTickers(cls):
        print('TestManageCandles.test_getTickers')

    @classmethod
    def test_cleanDuplicatesFromResults(cls):
        print('TestManageCandles.test_cleanDuplicatesFromResults')

    @unittest.skip("Broken and no pressing need to fix")
    def test_getReport(self):
        print('TestManageCandles.test_getReport')

    @classmethod
    def test_printLatestTimes(cls):
        print('TestManageCandles.test_printLatestTimes')

    @classmethod
    def test_getDeltaData(cls):
        print('TestManageCandles.test_getDeltaData')

    @classmethod
    def test_getFirstQuoteData(cls):
        print('TestManageCandles.test_getFirstQuoteData')


if __name__ == '__main__':
    unittest.main()
