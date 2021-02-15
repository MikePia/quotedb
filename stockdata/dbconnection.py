"""
Get the db connection string. Depends on the localconfig to get the keys
"""
import logging
import os
from models.managekeys import ManageKeys, Keys

sdb = 'sqlite:///keys.sqlite'

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
        d =  Keys.getKey('csv_directory', self.mk.engine)
        return d

    def getFhToken(self):
        '''Out of place but easy'''
        return Keys.getKey('fh_token', self.mk.engine)


        
def getCsvDirectory():
    msc = Mysqlconn()
    return msc.getCsvDirectory()

def getSaConn():
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


if __name__ == '__main__':
    print(getSaConn())

