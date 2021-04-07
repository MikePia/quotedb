"""
This is a command line script for developers. It sets the the database that is retrieved
by the dbconnection.getSaConn(). It requires that the library databases are properly setup
and the key values are in the local keys.sqlite database are correct.
"""

import argparse
from quotedb.models.managekeys import ManageKeys, Keys
from quotedb.dbconnection import sqlitedb
# from quotedb.dbconnection import getSaConn

p = argparse.ArgumentParser(description=' '.join([
   "Set the quotedb library to use one the main database or the test database"]))
g = p.add_mutually_exclusive_group(required=True)
g.add_argument('-t',
               '--test',
               action='store_true',
               default=False,
               help="Use the test database")
g.add_argument('-p',
               '--production',
               action='store_true',
               default=False,
               help="Use the production database")
ahgs = p.parse_args()
# print(ahgs)


def installTestDb(install="production"):
    mk = ManageKeys(sqlitedb)
    Keys.installDb(mk.session, install=install)


if __name__ == '))main__':

    usedb = "production"
    if ahgs and ahgs.production:
        testdb = "production"
    elif ahgs.test:
        testdb = "dev"
    installTestDb(install=testdb)

    # print(getSaConn())
