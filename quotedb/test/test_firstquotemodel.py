"""
The first MVP will take precedence over testing. But will add a few tests here and there.
"""

from pandas import Timestamp as ts

from quotedb.dbconnection import getSaConn
from quotedb.models.metamod import getSession, init, getEngine
from quotedb.utils.util import dt2unix
from quotedb.models.firstquotemodel import Firstquote, Firstquote_trades
from quotedb.models.managekeys import ManageKeys, Keys, constr

from unittest import TestCase


class TestFirstQuoteModel(TestCase):
    """
    Adds and deletes from the main database but uses the year 1980.  Viv l'MTV  la bas du Ronald Regan
    """
    def test_create(self):
        """
        Tests addFirstquote for creation and deleteFirstQuote
        """
        init()
        s = getSession()
        t = dt2unix(ts("19800303"))
        st = ['SNORK', 'KRONS', 'GETRICH', 'QUICK']
        op = [20, 30, 40, 50]
        hi = [30, 40, 50, 60]
        lo = [10, 20, 30, 40]
        cl = [20, 21, 22, 23]
        vo = [1234, 2345, 4567, 6789]
        candles = [Firstquote_trades(stock=s, open=o, high=h, low=lo, close=c, volume=v) for (s, o, h, lo, c, v) in zip(st, op, hi, lo, cl, vo)]

        Firstquote.addFirstquote(t,  candles, s)
        x = Firstquote.getFirstquote(t, s)
        self.assertEqual(x.timestamp, t)
        self.assertEqual(len(candles), len(x.firstquote_trades))
        Firstquote.deleteFirstquote(t, s)
        x = Firstquote.getFirstquote(t, s)
        self.assertIsNone(x)

    def test_update(self):
        """
        Test that Firstquote.addquotes will update  when the timestamp already exists
        """
        s = getSession()
        t = dt2unix(ts("19800303"))
        st = ['SNORK', 'KRONS', 'GETRICH', 'QUICK']
        op = [20, 30, 40, 50]
        hi = [30, 40, 50, 60]
        lo = [10, 20, 30, 40]
        cl = [20, 21, 22, 23]
        vo = [1234, 2345, 4567, 6789]
        candles = [Firstquote_trades(stock=s, open=o, high=h, low=lo, close=c, volume=v) for (s, o, h, lo, c, v) in zip(st, op, hi, lo, cl, vo)]

        Firstquote.addFirstquote(t,  candles, s)
        candles = candles[:-1]
        candles.append(Firstquote_trades(stock='DREEM', open=12, high=29, low=11.59, close=29, volume=9999999))
        Firstquote.addFirstquote(t, candles, s)
        x = Firstquote.getFirstquote(t, s)
        self.assertEqual(set([x.stock for x in candles]), set([z.stock for z in x.firstquote_trades]))
        Firstquote.deleteFirstquote(t, s)
        x = Firstquote.getFirstquote(t, s)
        self.assertIsNone(x)

    def test_tableCreation(self):
        mk = ManageKeys(constr)
        Keys.installDb(mk.session)
        init()
        self.assertIn('firstquote', getEngine().table_names())
        self.assertIn('firstquote_trades', getEngine().table_names())


if __name__ == '__main__':
    tfq = TestFirstQuoteModel()
    # tfq.test_update()
    tfq.test_tableCreation()
