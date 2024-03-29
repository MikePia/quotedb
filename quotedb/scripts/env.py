
import os


def getdbname():
    """
    Explanation
    -----------
    Establish the location of the sqlite keys database.
    The contents of the database contains keys and passwords and  must be populated seperatly.
    """
    fn = os.path.split(__file__)[0]
    fn = os.path.join(fn, '../')
    fn = os.path.join(fn, 'keys.sqlite')
    fn = os.path.normpath(fn)
    fn = 'sqlite:///' + fn
    return fn


sqlitedb = getdbname()


if __name__ == '__main__':
    print(sqlitedb)
