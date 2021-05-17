"""
Use a sqlite db to store tokens and keys including password
to the mysql db
"""
from dotenv import load_dotenv
import os
load_dotenv()

constr = None


class KeyItem:
    name = ''
    key = ''

    def __init__(self, key=None, name=None):
        self.key = key
        self.name = name

    def __repr__(self):
        return f"KeyItem<{self.key}, {self.name}"


class Keys():
    envmap = {
        'fh_token': 'FH_TOKEN1',
        'poly_token': 'POLY_TOKEN',
        'mysql_ip': 'DB_HOST',
        'mysql_port': 'DB_PORT',
        'mysql_user': 'DB_USER',
        'mysql_pw': 'DB_PASSWORD',
        'mysql_db': 'DB_NAME',
        'mysql_db_dev': 'DB_NAME_DEV',
        'mysql_user_dev': 'DB_USER_DEV',
        'mysql_pw_dev': 'DB_PASSWORD_DEV',
        'db_connect': 'DB_CONNECT',
        'project_dir': 'PROJECTDIR',
        'csv_directory': 'DATADIR',
        'db': 'DB_TYPE'
    }

    @classmethod
    def getKey(cls, key, session=None):
        """
        Use the  dev [db, user, pw] if cls.db is 'dev'
        """
        if os.environ['DB_TYPE'] == 'dev':
            if key in ['mysql_db', 'mysql_user', 'mysql_pw']:
                key += '_dev'
        return os.environ.get(cls.envmap[key], '')

    @classmethod
    def getAll(cls, session):
        """Writing this to match the sa interface return value-sortof"""
        keys = []
        for v in cls.envmap.values():
            keys.append(KeyItem(v, os.environ.get(v).strip()))
        return keys

    @classmethod
    def installDb(cls, install='dev'):
        """
        Explanation
        -----------
        Dynamically set the db, user and pw to the test db, user and password.
        Warning this is only as persistent as the os.environ variable. It relies on
        setting os.environ['DB_TYPE]
        """
        assert install in ['production', 'dev']
        os.environ['DB_TYPE'] = install


class ManageKeys:
    session = None

    def __init__(self, db=None, create=None):
        pass

    def createTables(self):
        pass


if __name__ == '__main__':
    mk = ManageKeys()
    for k in Keys.getAll(mk.session):
        print(k.key, ' ', k.name)
