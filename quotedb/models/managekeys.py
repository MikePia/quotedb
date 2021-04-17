"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
import logging
import pymysql
import sys
from sqlalchemy import create_engine, Column, String, Integer
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from quotedb.scripts.env import sqlitedb

constr = sqlitedb
Base = declarative_base()
Session = sessionmaker()

SESSION = None
ENGINE = None
SQLITE_DB_URL = sqlitedb


def init_sqlite():
    global ENGINE, Session, SESSION
    try:
        ENGINE = create_engine(SQLITE_DB_URL)
        Base.metadata.create_all(ENGINE)
        Session = sessionmaker(bind=ENGINE)
        SESSION = Session()
        print(f"initializing session for {SQLITE_DB_URL}")
    except OperationalError as ex:
        print('========================    Start the database please    =============================')
        print(ex)
        sys.exit()

    except Exception as ex:
        print('========================    RRRRRRRRR    =============================')
        print(ex, 'Exception in init of sqlite db')


def cleanup_sqlite(db=None):
    try:
        global SQLITE_DB_URL
        db = db if db else sqlitedb
        SQLITE_DB_URL = db
        SESSION.close()
        ENGINE.dispose()
    except Exception as ex:
        print(ex, 'Exception in cleanup')

def getEngine():
    global ENGINE
    if not ENGINE:
        init_sqlite()
    return ENGINE


def getSession(refresh=False):
    global SESSION
    if not SESSION or refresh:
        init_sqlite()
    return SESSION



class Keys(Base):
    __tablename__ = "keys"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    key = Column(String)

    @classmethod
    def getKey(cls, name, session):
        s = session
        q = s.query(Keys).filter_by(name=name).one_or_none()
        return q.key if q else None

    @classmethod
    def addKey(cls, name, key, session):
        """Add or update a key"""
        s = session
        q = s.query(Keys).filter_by(name=name).one_or_none()
        if q:
            q.key = key
            s.add(q)
        else:
            k = Keys(name=name, key=key)
            s.add(k)
        s.commit()

    @classmethod
    def removeKey(cls, name, session):
        s = session
        q = s.query(Keys).filter_by(name=name).one_or_none()
        if q:
            s.delete(q)
            s.commit()

    @classmethod
    def getAll(cls, session):
        s = session
        q = s.query(Keys).all()
        return q

    @classmethod
    def installDb(cls, install='dev'):
        """
        Explanation
        -----------
        Set the active db to either the testdb or the livedb. Store the livedb
        user, name and pw  in *_bak keys. Note that this relies on the intital setup
        to be correct.

        Programming Notes
        -----------------
        I don't have mysql permission to reuse my mysql username for a second database.
        So creating a dev database means creating a dev database user. I am hacking
        this to store the db, name, pw in 3 seperate backup keys.
        """
        # s = Session(bind=engine)
        init_sqlite()
        session = getSession()
        if install == 'dev':
            # Install the dev db, user and password as current
            Keys.addKey('mysql_db', Keys.getKey('mysql_db_dev', session), session)
            Keys.addKey('mysql_user', Keys.getKey('mysql_user_dev', session), session)
            Keys.addKey('mysql_pw', Keys.getKey('mysql_pw_dev', session), session)
        elif install == "production":
            # Install the main db, user and password as current
            Keys.addKey('mysql_db', Keys.getKey('mysql_db_bak', session), session)
            Keys.addKey('mysql_user', Keys.getKey('mysql_user_bak', session), session)
            Keys.addKey('mysql_pw', Keys.getKey('mysql_pw_bak', session), session)
        else:
            logging.info("Databse was not changed")
        cleanup_sqlite()


class ManageKeys:
    def __init__(self, db, create=False):
        '''
        :params db: a SQLalchemy sqlite connection string
        '''
        global SQLITE_DB_URL
        self.db = db
        SQLITE_DB_URL = db
        cleanup_sqlite(db)
        init_sqlite()
        self.engine = getEngine()
        self.session = getSession()
        if create:
            self.createTables()

    def createTables(self):
        # self.session = getSession()
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
    mk = ManageKeys(constr, True)
    for k in Keys.getAll(mk.session):
        print(k.name, k.key)
