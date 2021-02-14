"""
Get the db connection string. Depends on the localconfig to get the keys
"""
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

    def getFhToken(self):
        '''Out of place but easy'''
        return Keys.getKey('fh_token', self.mk.engine)

    def getSaMysqlConn(self):
        return f'mysql+pymysql:///{self.getUser()}:{self.getPw()}@{self.getHost()}/{self.getDb()}'


if __name__ == '__main__':
    msc = Mysqlconn()
    print(msc.getSaMysqlConn())

