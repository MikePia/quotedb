"""
Get the db connection string. Depends on the localconfig to get the keys
"""
from quotedb.models.managekeys import ManageKeys, Keys


class Mysqlconn:
    mk = ManageKeys()

    def getConnectionStr(self):
        return Keys.getKey('db_connect')

    def getHost(self):
        return Keys.getKey('mysql_ip')

    def getPort(self):
        return Keys.getKey('mysql_port')

    def getUser(self):
        return Keys.getKey('mysql_user')

    def getPw(self):
        return Keys.getKey('mysql_pw')

    def getDb(self):
        return Keys.getKey('mysql_db')

    def getSaMysqlConn(self):
        # return f'mysql+pymysql://{self.getUser()}:{self.getPw()}@{self.getHost()}/{self.getDb()}'
        return f'{self.getConnectionStr()}://{self.getUser()}:{self.getPw()}@{self.getHost()}/{self.getDb()}'

    # Couple of non mysql items
    def getCsvDirectory(self):
        d = Keys.getKey('csv_directory', self.mk.session)
        return d

    def getFhToken(self):
        '''Out of place but easy'''
        return Keys.getKey('fh_token', self.mk.session)

    def getPolygonToken(self):
        return Keys.getKey('poly_token', self.mk.session)


def getCsvDirectory():
    msc = Mysqlconn()
    return msc.getCsvDirectory()


def getSaConn(refresh=None):
    '''
    Get the Sqlalchemy Mysql connection string using the pymysql module
    '''
    msc = Mysqlconn()
    return msc.getSaMysqlConn()


def getFhToken():
    '''
    Get the finnhub token
    '''
    msc = Mysqlconn()
    return msc.getFhToken()


def getPolygonToken():
    msc = Mysqlconn()
    return msc.getPolygonToken()


__all__ = [getCsvDirectory, getSaConn, getFhToken, getPolygonToken]


if __name__ == '__main__':
    print(getCsvDirectory())
    print(getSaConn())
    print(getPolygonToken())
    print(getPolygonToken())
