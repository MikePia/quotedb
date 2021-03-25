"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
from .metamod import Base
from sqlalchemy import Column, String, Integer, Float


class CandlesModel(Base):
    __tablename__ = "candles"
    id = Column(Integer, primary_key=True)
    stock = Column(String(8), nullable=False, index=True)
    close = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    open = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False, index=True)
    volume = Column(Integer, nullable=False)


if __name__ == '__main__':
    pass
    # getRange()
    # mc = ManageCandles(getSaConn(), True)
    # mc = ManageCandles(getSaConn())
    # CandlesModel.printLatestTimes(nasdaq100symbols, mc.session)
    # mc.getLargestTimeGap('ZM')
    # mc.chooseFromReport(getCsvDirectory() + '/report.csv')
    # tickers = ['TXN', 'SNPS', 'SPLK', 'PTON', 'CMCSA', 'GOOGL']
    # mc.reportShape(tickers=mc.getQ100_Sp500())

    # #################################################################
    # from pprint import pprint
    # mc = ManageCandles(getSaConn())
    # start = dt2unix(pd.Timestamp(2021,  3, 16, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # print(pd.Timestamp(start, unit='s'))
    # # stocks = ['AAPL', 'SQ']
    # stocks = getQ100_Sp500()
    # gainers, losers = mc.filterGanersLosers(stocks, start, 10)
    #####################################
    # start = 1609463187
    # end = 1615943202
    # print(unix2date(start))
    # print(unix2date(end))
    # stocks = getQ100_Sp500()

    # mc = ManageCandles(getSaConn())
    # df = CandlesModel.getTimeRangeMultipleVpts(stocks, start, end, mc.session)
    #############################################
    # mc = ManageCandles(getSaConn(), create=True)
    # start = dt2unix(pd.Timestamp(2021,  3, 12, 12, 0, 0).tz_localize("US/Eastern").tz_convert("UTC").replace(tzinfo=None))
    # end = dt2unix(pd.Timestamp.utcnow().replace(tzinfo=None))
    # x = mc.getFilledData('AAPL', start, end)
