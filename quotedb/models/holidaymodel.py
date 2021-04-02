import csv
import datetime as dt
import pandas as pd

from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from quotedb.dbconnection import getSaConn

Base = declarative_base()
Session = sessionmaker()


class HolidayModel(Base):
    __tablename__ = "holidays"
    day = Column(String(8), nullable=False, primary_key=True)
    name = Column(String(104))

    def __repr__(self):
        return f'<holidays({self.name})>'

    @classmethod
    def insertHoliday(cls, day, name, engine):
        s = Session(bind=engine)
        q = s.query(HolidayModel).filter_by(day=day).one_or_none()
        if not q:
            h = HolidayModel(day=day, name=name)
            s.add(h)
            s.commit()

    @classmethod
    def insertHolidays(cls, dayarray, engine):
        s = Session(bind=engine)
        for day in dayarray:
            dadate = pd.Timestamp(day[0]).strftime("%Y%m%d")
            q = s.query(HolidayModel).filter_by(day=dadate).one_or_none()
            if not q:
                h = HolidayModel(day=dadate, name=day[1])
                s.add(h)
        s.commit()

    @classmethod
    def isHoliday(self, d, s):
        # s = Session(bind=engine)
        q = s.query(HolidayModel).filter_by(day=d.strftime("%Y%m%d")).one_or_none()
        return True if q else False


class ManageHolidayModel:
    session = None

    def __init__(self, db, create=False):
        self.db = db
        self.engine = create_engine(self.db)
        if create:
            self.createTables()
        self.newSession()

    def newSession(self):
        self.session = Session(bind=self.engine)

    def createTables(self):
        # s = Session(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def saveHolidays(self, fn):
        # This is an occasional thing during dev or setup

        csvfile = []
        with open(fn, 'r') as file:
            reader = csv.reader(file, dialect="excel")
            for row in reader:
                csvfile.append(row)
        HolidayModel.insertHolidays(csvfile, self.engine)


def test_isHoliday():
    mh = ManageHolidayModel(getSaConn())
    d = dt.date(2019, 1, 1)
    delt = dt.timedelta(days=1)
    s = Session(bind=mh.engine)
    for i in range(365):
        if HolidayModel.isHoliday(d, s):
            print(d.strftime("\n%A %B %d is a holiday"), end='')
        # time.sleep(0.2)
        print('.', end='')
        d += delt


if __name__ == '__main__':
    from quotedb.models.metamod import getSession

    # mh = ManageHolidayModel(getSaConn(), create=True)
    # mh.saveHolidays('quotedb/models/holidays.csv')
    # test_isHoliday()
    d = dt.date.today()
    print(HolidayModel.isHoliday(d, getSession()))
