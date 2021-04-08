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
               action='store_false',
               default=False,
               help="Use the test database")
g.add_argument('-p',
               '--production',
               action='store_true',
               default=False,
               help="Use the production database")


def installTestDb(install="production"):
    """
    Explanation
    -----------
    Install the dev database or production database as defined in keys.sqlite
    The command line usage:
        $ python installtestdb -t
        $ python installtestdb -p

    Paramaters
    -----------
    :params install: str: one of [production, dev]. Any other argument does nothing.
    """
    mk = ManageKeys(sqlitedb)
    Keys.installDb(mk.session, install=install)


if __name__ == '__main__':
    ahgs = p.parse_args()

    usedb = "production"
    if ahgs and ahgs.production:
        testdb = "production"
    elif ahgs.test:
        testdb = "dev"
    installTestDb(install=testdb)

    # print(getSaConn())
