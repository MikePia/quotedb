import logging
from sqlalchemy import create_engine

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

from quotedb.dbconnection import getSaConn
# from quotedb.sp500 import getQ100_Sp500


Base = declarative_base()
Session = None

SESSION = None
ENGINE = None
DB_URL = getSaConn()


def init():
    global ENGINE, Session, SESSION
    try:
        ENGINE = create_engine(DB_URL)
        Base.metadata.create_all(ENGINE)
        Session = scoped_session(sessionmaker(bind=ENGINE))
        SESSION = Session()
        db = "dev_stockdb" if DB_URL.find("dev_stockdb") > 0 else "stockddb"
        logging.debug(f"initializing session for {db}")
    except Exception as ex:
        print('========================    RRRRRRRRR    =============================')
        print(ex, 'Exception in init')


def cleanup():
    try:
        SESSION.close()
        ENGINE.dispose()
    except Exception as ex:
        print(ex, 'Exception in cleanup')


def getEngine():
    global ENGINE
    if not ENGINE:
        init()
    return ENGINE


def getSession(refresh=False):
    global SESSION
    if not SESSION or refresh:
        init()
    return SESSION
