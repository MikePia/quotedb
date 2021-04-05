import csv
import datetime as dt
import os

import pandas as pd
from quotedb.dbconnection import getCsvDirectory

EPOC = dt.datetime(1970, 1, 1)


def dt2unix(adate, unit='s'):
    # assert isinstance(adate, dt.datetime)
    if unit == 'n':
        return int((adate - EPOC).total_seconds() * 1000000000)

    if unit == 'm':
        return int((adate - EPOC).total_seconds() * 1000)

    return int((adate - EPOC).total_seconds())


def dt2unix_ny(x, unit='s'):
    x = pd.Timestamp(x)
    x = x.tz_localize('US/Eastern').tz_convert('UTC').replace(tzinfo=None)
    # x = x.tz_localize('US/Eastern').tz_convert('UTC').replace(tzinfo=None)
    return dt2unix(x, unit=unit)


def unix2date(u, unit='s'):
    '''s for seconds, m for microseconds, n for nanoseconds'''
    if unit == 'n':
        u = u/1000000000
    elif unit == 'm':
        u = u / 1000
    return EPOC + dt.timedelta(seconds=u)


def unix2date_ny(u, unit='s'):
    x = pd.Timestamp(unix2date(u, unit)).tz_localize("UTC").tz_convert('US/Eastern').replace(tzinfo=None)
    return x


def resample(df, col, rule, unit='s'):
    div = 1 if unit == 's' else 1000.0 if unit == 'ms' else 1000000000
    df = df.copy()

    # convert epoch times to datetime
    df['time'] = df.time.apply(
        lambda ts: dt.datetime.fromtimestamp(ts / div))

    # make the datetimes into an index
    df.set_index('time', inplace=True)

    # resample to desired period
    df = df.resample(rule).asfreq().reset_index()

    # convert datetimes back to epoch
    epoch = dt.datetime.fromtimestamp(0)
    df.time = df.time.apply(
        lambda ts: (ts - epoch).total_seconds())
    return df


def formatFn(fn, format):
    fn = f'{getCsvDirectory()}/{fn}'
    fn = os.path.splitext(fn)[0]
    fmat = '.csv' if format.lower() == 'csv' else '.json'
    d = dt.datetime.now()
    fn = f'{fn}_{d.strftime("%Y%m%d_%H%M%S")}{fmat}'
    return fn


def formatData(df, store, fill=False):
    """
    Paramaters
    ----------
    :params df: DataFrame. If store includes visualize, must include the collumns ['timestamp', 'stock']
    :params store: list.
    """
    if 'visualize' in store:
        if df.empty:
            return ''
        df.sort_values(['timestamp', 'stock'], inplace=True)
        visualize = []
        for t in df.timestamp.unique():
            # Note that t is a numpy.datetime. int(t) converts to Epoch in ns.
            tick = df[df.timestamp == t]
            cols = ['stock', 'price', 'volume']
            if fill:
                cols.extend(['delta_p', 'delta_t', 'delta_v'])

            visualize.append({int(int(t) / 1000000): tick[cols].to_json(orient="records")})
        return str(visualize)
    elif 'json' in store:
        if df.empty:
            return ''
        return df.to_json()
    elif 'csv' in store:
        if df.empty:
            return [[]]
        return df.to_csv(header=True)


def writeFile(j, fn, store):
    mode = 'a' if os.path.exists(fn) else 'w'
    if 'visualize' in store or 'json' in store:
        with open(fn, mode) as f:
            f.write(j)
    elif 'csv' in 'store':
        with open(fn, mode, newline='') as f:
            writer = csv.writer(f)
            for row in j:
                writer.writerow(row)


if __name__ == '__main__':
    # assert dt2unix(dt.datetime(2021, 2, 13)) == 1613174400
    # assert dt2unix(dt.datetime(2021, 2, 11)) == 1613001600
    # assert dt2unix(dt.datetime(2021, 2, 11)) != 1613001601
    print(unix2date(1613001600))
    print(unix2date_ny(1613001600))
