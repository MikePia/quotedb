"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

constr = "sqlite:///keys.sqlite"
Base = declarative_base()
Session = sessionmaker()


class Keys(Base):
    __tablename__ = "keys"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    key = Column(String)

    @classmethod
    def getKey(cls, name, engine):
        s = Session(bind=engine)
        q = s.query(Keys).filter_by(name=name).one_or_none()
        return q.key if q else None

    @classmethod
    def addKey(cls, name, key, engine):
        """Add or update a key"""
        s = Session(bind=engine)
        q = s.query(Keys).filter_by(name=name).one_or_none()
        if q:
            q.key = key
            s.add(q)
        else:
            k = Keys(name=name, key=key)
            s.add(k)
        s.commit()

    @classmethod
    def removeKey(cls, name, engine):
        s = Session(bind=engine)
        q = s.query(Keys).filter_by(name=name).one_or_none()
        if q:
            s.delete(q)
            s.commit()

    @classmethod
    def getAll(cls, engine):
        s = Session(bind=engine)
        q = s.query(Keys).all()
        return q


class ManageKeys:
    def __init__(self, db, create=False):
        '''
        :params db: a SQLalchemy sqlite connection string
        '''
        self.db = db
        self.engine = create_engine(self.db)
        if create:
            self.createTables()

    def createTables(self):
        self.session = Session(bind=self.engine)
        Base.metadata.create_all(self.engine)


def fortesting():
    import os
    p = constr.split('///')[1]
    if os.path.exists(p):
        try:
            os.remove(p)
        except PermissionError:
            print('PermissionError. close the gui thing already')


if __name__ == '__main__':
    mk = ManageKeys('sqlite:///test_keys.sqlite', True)
    Keys.getAll(mk.engine)
    print('done')
    print('done')
