"""
Get the db connection string. Depends on the localconfig to get the keys
"""
from models.managekeys import ManageKeys, Keys
from env import sqlitedb

sdb = sqlitedb
#  Make these sqlite accessors singletons
MYSQL_CON = None
FH_TOKEN = None
PG_TOKEN = None
CSV_DIRECTORY = None


class Mysqlconn:
    mk = ManageKeys(sdb)

    def getHost(self):
        return Keys.getKey('mysql_ip', self.mk.engine)

    def getPort(self):
        return Keys.getKey('mysql_port', self.mk.engine)

    def getUser(self):
        return Keys.getKey('mysql_user', self.mk.engine)

    def getPw(self):
        return Keys.getKey('mysql_pw', self.mk.engine)

    def getDb(self):
        return Keys.getKey('mysql_db', self.mk.engine)

    def getSaMysqlConn(self):
        return f'mysql+pymysql://{self.getUser()}:{self.getPw()}@{self.getHost()}/{self.getDb()}'

    # Couple of non mysql items
    def getCsvDirectory(self):
        d = Keys.getKey('csv_directory', self.mk.engine)
        return d

    def getFhToken(self):
        '''Out of place but easy'''
        return Keys.getKey('fh_token', self.mk.engine)

    def getPolygonToken(self):
        return Keys.getKey('poly_token', self.mk.engine)


# ====================== singleton accessors =========================
def getCsvDirectory():
    global CSV_DIRECTORY
    if CSV_DIRECTORY is None:
        msc = Mysqlconn()
        CSV_DIRECTORY = msc.getCsvDirectory()
    return CSV_DIRECTORY


def getSaConn():
    '''
    Get the Sqlalchemy Mysql connection string using the pymysql module
    '''
    global MYSQL_CON
    if MYSQL_CON is None:
        msc = Mysqlconn()
        MYSQL_CON = msc.getSaMysqlConn()
    return MYSQL_CON


def getFhToken():
    '''
    Get the finnhub token
    '''
    global FH_TOKEN
    if FH_TOKEN is None:
        msc = Mysqlconn()
        FH_TOKEN = msc.getFhToken()
    return FH_TOKEN


def getPolygonToken():
    global PG_TOKEN
    if PG_TOKEN is None:
        msc = Mysqlconn()
        PG_TOKEN = msc.getPolygonToken()
    return PG_TOKEN


__all__ = [getCsvDirectory, getSaConn, getFhToken, getPolygonToken]


if __name__ == '__main__':
    print(getSaConn())
    print(getPolygonToken())
    print(getPolygonToken())
