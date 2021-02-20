import datetime as dt

EPOC = dt.datetime(1970,1,1)

def dt2unix(adate):
    # assert isinstance(adate, dt.datetime)
    return int((adate - EPOC).total_seconds())

def unix2date(u):
    return EPOC + dt.timedelta(seconds=u)

if __name__ == '__main__':
    assert dt2unix(dt.datetime(2021,2,13)) == 1613174400
    assert dt2unix(dt.datetime(2021,2,11)) == 1613001600
    assert dt2unix(dt.datetime(2021,2,11)) != 1613001601
