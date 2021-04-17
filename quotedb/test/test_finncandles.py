import datetime as dt
import os
import random
from unittest import TestCase
import unittest

from quotedb.dbconnection import getSaConn
from quotedb.finnhub.finncandles import FinnCandles

from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.topquotes_candlemodel import TopquotesModel
from quotedb.models.metamod import getEngine, init, cleanup

from quotedb.scripts.installtestdb import installTestDb
from quotedb.utils.util import getPrevTuesWed, dt2unix, dt2unix_ny


def dblcheckDbMode(db=None, reverse=False):
    """In case this is called without setUpClass(), and the db is not in test mode,fail with AssertionError """
    db = db if db else getSaConn()
    if reverse:
        assert db.find("dev_stockdb") < 0
    else:
        assert db.find("dev_stockdb") > 0


class TestFinnCandles(TestCase):
    stocks = None
    start = None

    @classmethod
    def setUpClass(cls):
        print("setUpClass TestFinnCandles")
        installTestDb(install="dev")
        init()
        db = getSaConn(refresh=True)
        dblcheckDbMode(db)
        engine = getEngine()
        statements = ['delete from candles', 'delete from allquotes', 'delete from topquotes']
        for statement in statements:
            engine.execute(statement)

        cls.stocks = ['AAPL', 'SQ', 'XPEV', 'TSLA']
        d = getPrevTuesWed(dt.datetime.now())
        cls.start = dt2unix_ny(dt.datetime(d.year, d.month, d.day, 10, 30))

        fc = FinnCandles(cls.stocks)
        fc.cycleStockCandles(cls.start, numcycles=0)

    @classmethod
    def tearDownClass(cls):
        print("tearDownClass TestFinnCandles()")
        installTestDb(install="production")
        db = getSaConn(refresh=True)
        print('Resetting db to stockdb')
        dblcheckDbMode(db, reverse=True)

    def test_getCandles_fh(self):
        print("test_getCandles_fh()")
        fc = FinnCandles(tickers=self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        start = self.start - 60*60*2
        end = self.start
        candles = fc.getCandles_fh(stock, start, end)
        self.assertIsNotNone(candles)
        self.assertIsInstance(candles, dict)

    def test_storeCandles_db_candles(self):
        """
        Test the storage to the candles table
        Test relies on getting and storing candles earlier than setUpClass did.
        """
        print("test_storeCandles_db_candles()")
        dblcheckDbMode()
        model = CandlesModel
        fc = FinnCandles(tickers=self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        start = self.start - 60*60*2
        end = dt2unix(dt.datetime.utcnow())
        fc.storeCandles(stock, end, start=start, model=model, store=['db'])
        mc = fc.getManageCandles(model, reinit=True)
        candles = mc.getTimeRange(stock, start, end)
        self.assertIsInstance(candles, list)
        self.assertGreater(len(candles), 0)
        self.assertIsInstance(candles[0], model)

    def test_storeCandles_db_allquotes(self):
        """
        Test the storage to the allquotes table
        Test relies on getting and storing candles earlier than setUpClass did.
        """
        print("test_storeCandles_db_allquotes()")
        dblcheckDbMode()
        model = AllquotesModel
        fc = FinnCandles(tickers=self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        start = self.start - 60*60*2
        end = dt2unix(dt.datetime.utcnow())
        fc.storeCandles(stock, end, start=start, model=model, store=['db'])
        mc = fc.getManageCandles(model, reinit=True)
        candles = mc.getTimeRange(stock, start, end)
        self.assertIsInstance(candles, list)
        self.assertGreater(len(candles), 0)
        self.assertIsInstance(candles[0], model)

    def test_storeCandles_db_topquotes(self):
        """
        Explanation
        -----------
        Test the storage to the topquotes table.
        """
        print("test_storeCandles_db_topquotes()")
        dblcheckDbMode()
        model = TopquotesModel
        fc = FinnCandles(tickers=self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        start = self.start - 60*60*2
        end = dt2unix(dt.datetime.utcnow())
        fq_time = start
        fc.storeCandles(stock, end, start=start, model=model, store=['db'], fq_time=fq_time)
        mc = fc.manageCandles
        candles = mc.getTimeRange(stock, start, end)
        self.assertIsInstance(candles, list)
        self.assertGreater(len(candles), 0)
        self.assertIsInstance(candles[0], model)

    def test_storeCandles_csv(self):
        """
        Test the storage to a csv file
        """
        print("test_storeCandles_csv()")
        dblcheckDbMode()
        model = CandlesModel
        fc = FinnCandles(tickers=self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        start = self.start - 60*60*2
        end = dt2unix(dt.datetime.utcnow())
        fn = fc.storeCandles(stock, end, start=start, model=model, store=['csv'])
        self.assertTrue(os.path.exists(fn))

    def test_storeCandles_json(self):
        """
        Test the storage to a json file
        """
        print("test_storeCandles_json()")
        dblcheckDbMode()
        model = CandlesModel
        fc = FinnCandles(tickers=self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        start = self.start - 60*60*2
        end = dt2unix(dt.datetime.utcnow())
        fn = fc.storeCandles(stock, end, start=start, model=model, store=['json'])
        self.assertTrue(os.path.exists(fn))

    def test_cycleStockCandles_candles(self):
        print("test_cycleStockCandles_candles()")
        dblcheckDbMode()
        init()
        model = CandlesModel
        statements = ['delete from candles']

        for statement in statements:
            getEngine().execute(statement)
        fc = FinnCandles(self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        mc = fc.getManageCandles(model, reinit=True)
        end = dt2unix(dt.datetime.utcnow())
        candles = mc.getTimeRange(stock, self.start, end)
        self.assertListEqual(candles, [])
        cleanup()
        init()

        mc = fc.getManageCandles(model, reinit=True)
        fc.cycleStockCandles(self.start, model=model, numcycles=0)
        candles = mc.getTimeRange(stock, self.start, end)
        self.assertIsInstance(candles, list)
        self.assertGreater(len(candles), 0)
        self.assertIsInstance(candles[0], model)

        cleanup()

    def test_cycleStockCandles_allquotes(self):
        print("test_cycleStockCandles_allquotes()")
        dblcheckDbMode()
        init()
        model = AllquotesModel
        statements = ['delete from allquotes']

        for statement in statements:
            getEngine().execute(statement)
        fc = FinnCandles(self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        mc = fc.getManageCandles(model, reinit=True)
        end = dt2unix(dt.datetime.utcnow())
        candles = mc.getTimeRange(stock, self.start, end)
        self.assertListEqual(candles, [])
        cleanup()
        init()

        mc = fc.getManageCandles(model, reinit=True)
        fc.cycleStockCandles(self.start, model=model, numcycles=0)
        candles = mc.getTimeRange(stock, self.start, end)
        self.assertIsInstance(candles, list)
        self.assertGreater(len(candles), 0)
        self.assertIsInstance(candles[0], model)

        cleanup()

    def test_cycleStockCandles_topquotes(self):
        print("test_cycleStockCandles_topquotes()")
        dblcheckDbMode()
        init()
        model = TopquotesModel
        # Empty all calndle tables, verify topquotes is empty
        statements = ['delete from topquotes']

        for statement in statements:
            getEngine().execute(statement)
        fc = FinnCandles(self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        mc = fc.getManageCandles(model, reinit=True, fq_time=-1)
        end = dt2unix(dt.datetime.utcnow())
        candles = mc.getTimeRange(stock, self.start, end)
        self.assertListEqual(candles, [])
        cleanup()
        init()

        # Repopulate allquotes first
        # fc.cycleStockCandles(self.start, model=AllquotesModel, numcycles=0)

        # Establish fq in topqutes and Populate topquotes
        mc = fc.getManageCandles(model, reinit=True, fq_time=self.start)
        fc.cycleStockCandles(self.start, model=model, numcycles=0)
        candles = mc.getTimeRange(stock, self.start, end)
        self.assertIsInstance(candles, list)
        self.assertGreater(len(candles), 0)
        self.assertIsInstance(candles[0], model)

        cleanup()

    def test_getDateRange(self):
        print("\ntest_getDateRange_candles()")

        fc = FinnCandles(self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        start = dt2unix(getPrevTuesWed(dt.datetime.now()))
        end = dt2unix(dt.datetime.utcnow())
        j = fc.getDateRange(stock, start, end)
        self.assertIsInstance(j, list)
        self.assertGreater(len(j), 0)
        self.assertEqual(len(j[0]), 6)
        print()

    def test_getManageCandles(self):
        from quotedb.models.managecandles import ManageCandles
        from quotedb.models.managetopquotes import ManageTopQuote
        print("\ntest_getManageCandles()")
        fc = FinnCandles(self.stocks)

        mc = fc.getManageCandles(CandlesModel, reinit=True)
        self.assertIsInstance(fc.manageCandles, ManageCandles)
        self.assertEqual(mc.model.__tablename__, "candles")

        mc = fc.getManageCandles(AllquotesModel, reinit=True)
        self.assertIsInstance(fc.manageCandles, ManageCandles)
        self.assertEqual(mc.model.__tablename__, "allquotes")

        mc = fc.getManageCandles(TopquotesModel, reinit=True, fq_time=self.start)
        self.assertIsInstance(fc.manageCandles, ManageTopQuote)
        self.assertEqual(mc.model.__tablename__, "topquotes")
        self.assertIsInstance(mc.fq, tuple)
        self.assertIsInstance(mc.fq[1], int)
        self.assertIsInstance(mc.fq[0], dict)

    def test_getSymbols(self):
        print("test_getSymbols()")
        print('bogus test')


if __name__ == '__main__':
    unittest.main()
