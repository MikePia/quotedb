import datetime as dt

EPOC = dt.datetime(1970,1,1)

def dt2unix(adate):
    # assert isinstance(adate, dt.datetime)
    return int((adate - EPOC).total_seconds())

def unix2date(u):
    return EPOC + dt.timedelta(seconds=u)


def resample(df, col, rule):
    df = df.copy()

    # convert epoch times to datetime
    df.time = df.time.apply(
        lambda ts: dt.datetime.fromtimestamp(ts))

    # make the datetimes into an index
    df.set_index(col, inplace=True)

    # resample to desired period
    df = df.resample(rule).asfreq().reset_index()

    # convert datetimes back to epoch
    epoch = dt.datetime.fromtimestamp(0)
    df.time = df.time.apply(
        lambda ts: (ts - epoch).total_seconds())
    return df


if __name__ == '__main__':
    assert dt2unix(dt.datetime(2021,2,13)) == 1613174400
    assert dt2unix(dt.datetime(2021,2,11)) == 1613001600
    assert dt2unix(dt.datetime(2021,2,11)) != 1613001601
