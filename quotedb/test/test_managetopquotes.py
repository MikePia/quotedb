"""Test the methods in ManageTopquotes class.

ManageTopquotes implemnets an selection of methods from ManageCandles. It is an interface
pattern without external enforcement.

The elaborate setup required should reflect day to day operations but is a weakness in
the library.
"""
import datetime as dt
import random

import pandas as pd
import unittest
from unittest import TestCase

from quotedb.dbconnection import getSaConn
from quotedb.finnhub.finncandles import FinnCandles
from quotedb.models.common import dblcheckDbMode, createFirstQuote
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.firstquotemodel import Firstquote, Firstquote_trades
from quotedb.models.managetopquotes import ManageTopQuote
from quotedb.models import metamod as mm
from quotedb.models.topquotes_candlemodel import TopquotesModel
from quotedb.scripts.installtestdb import installTestDb
from quotedb.sp500 import random50
from quotedb.utils import util


class TestManageTopquotes(TestCase):

    # Create basic dates and choose stocks to test
    sstocks = random50(numstocks=5)
    stocks = sstocks[:4]
    extrastock = sstocks[4]
    d = util.getPrevTuesWed(dt.datetime.now())
    start = util.dt2unix_ny(dt.datetime(d.year, d.month, d.day, 9, 30))
    end = util.dt2unix(dt.datetime.utcnow())
    fq_time = start - 60 * 60 * 2

    @classmethod
    def setUpClass(cls):
        """Prepare and use the test database dev_stockdb."""

        # Install testdb
        installTestDb(install="dev")
        db = getSaConn(refresh=True)
        dblcheckDbMode(db)
        mm.init()
        mm.getEngine().execute("delete from topquotes")
        mm.cleanup()

        # Add data to allquotes for our quotes to provide for firstquote data
        fc = FinnCandles(cls.stocks)
        fc.cycleStockCandles(cls.start - 60*60*18, AllquotesModel, numcycles=0)

        # Reinitialize topquotes and add some data
        mm.init()
        fc.cycleStockCandles(cls.start, model=TopquotesModel, numcycles=0, fq_time=cls.fq_time)

        # Verify we got data or fail and skip these tests
        mtq = ManageTopQuote(cls.stocks, TopquotesModel, create=True, fq_time=cls.fq_time)
        stock = cls.stocks[random.randint(0, len(cls.stocks)-1)]
        mtq.getTimeRange(stock, cls.start, cls.end)
        tmax = mtq.getMaxTime(stock,  mm.getSession())
        tmin = mtq.getMinTime(stock,  mm.getSession())
        assert tmax and tmin, "<Programmer assertion> Basic setup of top quotes failed in setUpClass."
        assert (tmax - tmin) > 60 * 60 * 24, "<Programmer assertion> Basic setup of top quotes failed in setUpClass."

    @classmethod
    def tearDownClass(cls):
        installTestDb(install="production")
        mm.cleanup()
        mm.init()
        dblcheckDbMode(reverse=True)

    @unittest.skip("The reason for failur is firstquote lacks and update of topquote")
    def test_addCandles(self):

        fc = FinnCandles(self.stocks)

        start = self.start - (60*60*48)
        end = util.dt2unix(dt.datetime.utcnow())
        candles = fc.getCandles_fh(self.extrastock, start, end)
        mtq = ManageTopQuote(self.stocks, TopquotesModel)
        df = pd.DataFrame(candles)
        df.rename(columns={'c': 'close', 'h': 'high', 'l': 'low', 'o': 'open', 's': 'status', 't': 'timestamp', 'v': 'volume'}, inplace=True)
        mtq.addCandles(self.extrastock, df, mm.getSession())
        candles = mtq.getTimeRange(self.extrastock, start, end)
        self.assertIsInstance(candles, [])
        self.assertGreater(len(candles, 1))
        self.assertIsInstance(candles[0], TopquotesModel)

    def test_createTables(self):
        tables = ['topquotes', 'allquotes']
        # Delete and restore tables
        mm.init()
        tabnames = mm.getEngine().table_names()
        for table in tables:
            self.assertIn(table, tabnames)

    def test_getFirstQuote(self):
        """Test getting an existant firstquote

        This test relies on firstquote existing already. And also tests that it is
        the same collection of stocks that are created in setUpClass
        """
        mt = ManageTopQuote(self.stocks, TopquotesModel)
        fq = mt.getFirstquote()
        self.assertIsInstance(fq, Firstquote)
        self.assertIsInstance(fq.firstquote_trades, list)
        self.assertGreater(len(fq.firstquote_trades), 0)
        self.assertIsInstance(fq.firstquote_trades[0], Firstquote_trades)
        self.assertTrue(set(self.stocks).issubset(set([x.stock for x in fq.firstquote_trades])))

    def test_getMaxTime(self):
        mt = ManageTopQuote(None, TopquotesModel)
        stocks = mt.getTickers()
        stock = stocks[random.randint(0, len(stocks)-1)]
        maxt = mt.getMaxTime(stock, mt.session)
        self.assertIsInstance(maxt, int)

    def test_getMaxTimeForEachTicker(self):
        mt = ManageTopQuote(self.stocks, TopquotesModel)
        maxtimes = mt.getMaxTimeForEachTicker(self.stocks)
        self.assertEqual(len(maxtimes), len(self.stocks))

    def test_getMinTime(self):
        mt = ManageTopQuote(None, TopquotesModel)
        stocks = mt.getTickers()
        stock = stocks[random.randint(0, len(stocks)-1)]
        maxt = mt.getMinTime(stock, mt.session)
        self.assertIsInstance(maxt, int)

    def test_getTickers(self):
        mt = ManageTopQuote(self.stocks, TopquotesModel)
        stocks = mt.getTickers()
        sstocks = self.stocks
        if len(stocks) > len(self.stocks):
            sstocks.append(self.extrastock)
        self.assertEqual(set(sstocks), set(stocks))

    def test_getTimeRange(self):
        mt = ManageTopQuote(self.stocks, TopquotesModel)
        start = self.start
        end = start + (4*60*60)
        stock = self.stocks[random.randint(0, len(self.stocks)-1)]
        results = mt.getTimeRange(stock, start, end)
        self.assertIsInstance(results, list)
        self.assertGreater(len(results), 0)
        self.assertIsInstance(results[0], TopquotesModel)
        times = sorted([x.timestamp for x in results])
        self.assertGreaterEqual(times[0], start)
        self.assertLessEqual(times[-1], end)

    def test_installFirstQuote_nonvolatile(self):

        mtm = ManageTopQuote(self.stocks, TopquotesModel, fq_time=-1)
        fq = createFirstQuote(self.fq_time, AllquotesModel, self.stocks, local=True)
        mtm.installFirstQuote(self.stocks, fq=fq)
        # self.assertTrue(False)

    @unittest.skip("Update is not implemented")
    def test_updateFirstQuote(self):
        print("test_updateFirstQuote")
        # self.assertTrue(False)


if __name__ == '__main__':
    unittest.main()
    # TestManageTopquotes.setUpClass()
    # tmt = TestManageTopquotes()
    # # tmt.test_addCandles()
    # tmt.test_createTables()
    # # tmt.test_getFirstQuote()
    # tmt.test_getMaxTime()
    # tmt.test_getMinTime()
    # # tmt.test_getTickers()
    # # tmt.test_getTimeRange()
    # tmt.test_installFirstQuote_nonvolatile()
