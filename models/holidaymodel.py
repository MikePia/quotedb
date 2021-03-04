import csv
import datetime as dt
import pandas as pd

from sqlalchemy import create_engine, Column, String, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from stockdata.dbconnection import getSaConn

Base = declarative_base()
Session = sessionmaker()

class HolidayModel(Base):
    __tablename__ = "holidays"
    day = Column(Date, nullable=False, primary_key=True)
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
            dadate = pd.Timestamp(day[0]).date()
            q = s.query(HolidayModel).filter_by(day=dadate).one_or_none()
            if not q:
                h = HolidayModel(day=dadate, name=day[1])
                s.add(h)
        s.commit()

    @classmethod
    def isHoliday(self, d, engine):
        s = Session(bind=engine)
        q = s.query(HolidayModel).filter_by(day=d).one_or_none()
        return True if q else False


class ManageHolidayModel:

    def __init__(self, db, create=False):
        self.db = db
        self.engine = create_engine(self.db)
        if create:
            self.createTables()

    def createTables(self):
        s = Session(bind=self.engine)
        Base.metadata.create_all(self.engine)

    def saveHolidays(self, fn):
        # This is an occasional thing during dev or setup

        csvfile = []
        with open(fn, 'r') as file:
            reader = csv.reader(file, dialect="excel")
            for row in reader:
                csvfile.append(row)
        HolidayModel.insertHolidays(csvfile, self.engine)

        
if __name__ == '__main__':
    mh = ManageHolidayModel(getSaConn())
    mh.saveHolidays('models/holidays.csv')



    