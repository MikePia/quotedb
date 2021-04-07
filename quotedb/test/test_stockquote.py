"""
Test the REST api calls in StockQuote
"""
import sys
import unittest
from unittest import TestCase
from quotedb.dbconnection import getSaConn


class TestStockQuote(TestCase):

    def __init__(self, *args, **kwargs):
        """
        Quit if we are on Windows and not using localhost for the db
        """
        super(TestStockQuote, self).__init__(*args, **kwargs)
        if sys.platform == 'win32':
            if getSaConn().find('localhost') < 0:
                print('FATAL ERROR: TestStockQuote. On Windows but not using localhost')
                sys.exit()

    @classmethod
    def setUpClass(cls):
        cls.symbols = ['AAPL', 'ROKU', 'APHA', 'SQ', 'BBB']

        return super().setUpClass()

    # def test_getCandlesStart(self):
    #     sq = StockQuote()
    #     print('hello fred')
    #     start = dt2unix(dt.datetime.now() - dt.timedelta(days=14))
    #     end = dt2unix(dt.datetime.now() - dt.timedelta(days=7))
    #     symbol = self.symbols[random.randrange(len(self.symbols))]
    #     j = sq.getCandles(symbol, start, end, 1)
    #     sq.getQuote()
    #     self.assertGreater(len(j), 100)
    #     mintime, maxtime = j['t'][0],  j['t'][-1]
    #     self.assertGreaterEqual(mintime, start)
    #     self.assertLessEqual(maxtime, end)
    #     print()


if __name__ == '__main__':
    unittest.main()
