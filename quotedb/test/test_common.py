'''
Test the First quote access with 2 models and their manager class
'''
import pandas as pd
from quotedb.dbconnection import getSaConn
from quotedb.models.allquotes_candlemodel import AllquotesModel
from quotedb.models.candlesmodel import CandlesModel

from quotedb.models.common import createFirstQuote, getFirstQuoteData
from quotedb.models.firstquotemodel import Firstquote
from quotedb.models.managecandles import ManageCandles

from quotedb.models.metamod import getSession, cleanup, init

# import unittest
from unittest import TestCase
import unittest


class TestCommon(TestCase):

    @classmethod
    def setUpClass(cls):
        print("TestCommon.setUpClass()")
        s = getSession(refresh=True)
        mkq = ManageCandles(getSaConn(), AllquotesModel)
        cls.maxaapl_q = mkq.getMaxTime('AAPL', s)
        cls.minaapl_q = mkq.getMinTime('AAPL', s)
        assert cls.maxaapl_q
        assert cls.minaapl_q
        assert (cls.maxaapl_q - cls.minaapl_q) > 60 * 60 * 24
        cleanup()

        s = getSession(refresh=True)
        mkc = ManageCandles(None, CandlesModel)
        cls.maxaapl_c = mkc.getMaxTime('AAPL', s)
        cls.minaapl_c = mkc.getMinTime('AAPL', s)
        assert cls.maxaapl_c
        assert cls.minaapl_c
        assert (cls.maxaapl_c - cls.minaapl_c) > 60 * 60 * 24
        cleanup()
        # If this fails, the db is pretty empty

    def test_getFirstQuoteData_allquote(self):
        print("test_getFirstquote_allquotes()")
        init()
        start = self.maxaapl_q - (60*60*2)
        thestocks = ['AAPL', "TSLA", "ROKU"]
        fqd = getFirstQuoteData(start, AllquotesModel.__tablename__, thestocks=thestocks)
        self.assertIsInstance(fqd, pd.DataFrame)
        msg = "A failure here may mean the stocks were not all in the database"
        self.assertEqual(len(fqd), len(thestocks), msg)
        cleanup()

    def test_getFirstQuoteData_candles(self):
        print("test_getFirstquote_candles()")
        init()
        start = self.maxaapl_c - (60*60*2)
        thestocks = ['AAPL', "TSLA", "ROKU"]
        fqd = getFirstQuoteData(start, CandlesModel.__tablename__, thestocks=thestocks)
        self.assertIsInstance(fqd, pd.DataFrame)
        msg = "A failure here may mean the stocks were not all in the database"
        self.assertEqual(len(fqd), len(thestocks), msg)
        cleanup()

    def test_createFirstquote_allquotes(self):
        """
        Explanation
        -----------
        Provide a date to get the data for Firstquote.
        """
        print("test_createFirstquote_allquotes()")
        s = getSession(refresh=True)
        start = self.maxaapl_q - (60*60*2)

        stocks = ['AAPL', "TSLA", "ROKU"]
        while True:
            # Find a blank spot to create first quote
            fq = Firstquote.getFirstquote(start, s)
            if fq:
                start += 60
            else:
                break
        fq = createFirstQuote(start, AllquotesModel, stocks=stocks, usecache=True)
        self.assertIsInstance(fq, Firstquote)
        self.assertEqual(len(fq.firstquote_trades), len(stocks))
        cleanup()
        s = getSession(refresh=True)
        Firstquote.deleteFirstquote(start, s)
        fq = Firstquote.getFirstquote(start, s)
        self.assertIsNone(fq)
        cleanup()

    def test_createFirstquote_candles(self):
        """
        Explanation
        -----------
        Provide a date to get the data for Firstquote.
        """
        print("test_createFirstquote_candles()")
        s = getSession(refresh=True)
        start = self.maxaapl_c - (60*60*2)
        stocks = ['AAPL', "TSLA", "ROKU"]
        while True:
            # Find a blank spot to create first quote
            fq = Firstquote.getFirstquote(start, s)
            if fq:
                start += 60
            else:
                break
        fq = createFirstQuote(start, CandlesModel, stocks=stocks)
        self.assertIsInstance(fq, Firstquote)
        self.assertEqual(len(fq.firstquote_trades), len(stocks), "A failures here may mean the db just doesn't have the candles")
        cleanup()
        s = getSession(refresh=True)
        Firstquote.deleteFirstquote(start, s)
        fq = Firstquote.getFirstquote(start, s)
        self.assertIsNone(fq)
        cleanup()


if __name__ == '__main__':
    unittest.main()
    # TestCommon.SetUpClass()
    # tc = TestCommon()
    # # tc.test_getFirstQuoteData()
    # tc.test_createFirstquote_bydate()
    # print()
