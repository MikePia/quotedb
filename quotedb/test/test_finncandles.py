import datetime as dt
import os
import random
from unittest import TestCase
import unittest

from quotedb.dbconnection import getSaConn, getCsvDirectory
from quotedb.finnhub.finncandles import FinnCandles

from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.metamod import getEngine

from quotedb.scripts.installtestdb import installTestDb
from quotedb.utils.util import getPrevTuesWed, dt2unix, dt2unix_ny


class TestFinnCandles(TestCase):
    stocks = None
    start = None

    @classmethod
    def setUpClass(cls):
        installTestDb(install="dev")
        db = getSaConn(refresh=True)
        assert db.find("dev_stockdb") > 0
        engine = getEngine()
        statements = ['delete from candles', 'delete from allquotes']
        for statement in statements:
            engine.execute('delete from candles')

        cls.stocks = ['AAPL', 'SQ', 'XPEV', 'TSLA']
        d = getPrevTuesWed(dt.datetime.now())
        cls.start = dt2unix_ny(dt.datetime(d.year, d.month, d.day, 10, 30))

        fc = FinnCandles(cls.stocks)
        fc.cycleStockCandles(cls.start, numcycles=1)

    @classmethod
    def tearDownClass(cls):
        installTestDb(install="production")
        db = getSaConn(refresh=True)
        print('Resetting db to stockdb')
        assert db.find("dev_stockdb") < 0

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
        print("test_storeCandles_db_candles")
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
        Test the storage to the candles table
        Test relies on getting and storing candles earlier than setUpClass did.
        """
        print("test_storeCandles_db_allquotes")
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

    def test_storeCandles_csv(self):
        """
        Test the storage to a csv file
        """
        print("test_storeCandles_csv")
        # hard coding the default resolution here. Not sure if it will ever change
        resolution = 1
        model = CandlesModel
        fc = FinnCandles(tickers=self.stocks)
        stock = fc.tickers[random.randint(0, len(self.stocks)-1)]
        start = self.start - 60*60*2
        end = dt2unix(dt.datetime.utcnow())
        fc.storeCandles(stock, end, start=start, model=model, store=['csv'])
        fn = getCsvDirectory() + f'/{stock}_{fc.cycle[stock]}_{end}_{resolution}.csv'
        self.assertTrue(os.path.exists(fn))
        print()

    def test_cycleStockCandles(self):
        print('bogus test')

    def test_getDateRange(self):
        print('bogus test')

    def test_getManageCandles(self):
        print('bogus test')

    def test_getSymbols(self):
        print('bogus test')


if __name__ == '__main__':
    unittest.main()
