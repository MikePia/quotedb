"""
This is a command line script for developers. It sets the the database that is retrieved
by the dbconnection.getSaConn(). It requires that the library databases are properly setup
and the key values are in the local keys.sqlite database are correct.
"""

import argparse
from quotedb.models.managekeys import Keys
from quotedb.dbconnection import getSaConn

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
g.add_argument('-s',
               '--show',
               action='store_true',
               default=False,
               help="Show which database is active")


def installTestDb(install="production", show=False):
    """
    Explanation
    -----------
    Install the dev database or production database as defined in keys.sqlite
    The command line usage:
        $ python installtestdb -t -- Use test database
        $ python installtestdb -p -- Use production database
        $ python installtestdb -c -- Show active database

    Paramaters
    -----------
    :install: str: one of [production, dev]. Any other argument does nothing.
    :show: bool: If True, print current db string and exit

    """
    if show:
        db = getSaConn()
        db = "dev_stockdb" if db.find("dev_stockdb") > 0 else 'stockdb'
        msg = f'{db} is the current database'
        print(msg)
        return msg
    Keys.installDb(install=install)
    db = getSaConn(refresh=True)
    db = "dev_stockdb" if db.find("dev_stockdb") > 0 else 'stockdb'
    print(f'{db} is the current database')


def main(ahgs):
    testdb = "production"
    show = False
    if ahgs and ahgs.production:
        testdb = "production"
    elif ahgs.test:
        testdb = "dev"
    if ahgs.show:
        show = True

    return installTestDb(install=testdb, show=show)


if __name__ == '__main__':
    ahgs = p.parse_args()
    main(ahgs)
