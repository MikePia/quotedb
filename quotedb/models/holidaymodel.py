import csv
import pandas as pd

from sqlalchemy import Column, String
from quotedb.models.metamod import Base, init, getSession
from sqlalchemy.orm import sessionmaker

Session = sessionmaker()


class HolidayModel(Base):
    __tablename__ = "holidays"
    day = Column(String(8), nullable=False, primary_key=True)
    name = Column(String(104))

    def __repr__(self):
        return f'<holidays({self.name})>'

    @classmethod
    def insertHoliday(cls, day, name, session):
        s = session
        q = s.query(HolidayModel).filter_by(day=day).one_or_none()
        if not q:
            h = HolidayModel(day=day, name=name)
            s.add(h)
            s.commit()

    @classmethod
    def insertHolidays(cls, dayarray, session):
        s = session
        for day in dayarray:
            dadate = pd.Timestamp(day[0]).strftime("%Y%m%d")
            q = s.query(HolidayModel).filter_by(day=dadate).one_or_none()
            if not q:
                h = HolidayModel(day=dadate, name=day[1])
                s.add(h)
        s.commit()

    @classmethod
    def isHoliday(self, d, s):
        q = s.query(HolidayModel).filter_by(day=d.strftime("%Y%m%d")).one_or_none()
        return True if q else False


class ManageHolidayModel:
    session = None

    def __init__(self, create=False):
        if create:
            self.createTables()
        self.newSession()

    def newSession(self):
        self.session = getSession(refresh=True)

    def createTables(self):
        init()

    def saveHolidays(self, fn):
        # This is an occasional thing during dev or setup

        csvfile = []
        with open(fn, 'r') as file:
            reader = csv.reader(file, dialect="excel")
            for row in reader:
                csvfile.append(row)
        HolidayModel.insertHolidays(csvfile, self.session)


if __name__ == '__main__':
    # import os
    d = pd.Timestamp(2021, 7, 5)
    print(HolidayModel.isHoliday(d,  getSession()))
