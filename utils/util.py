import csv
import datetime as dt
import json
import os

import pandas as pd
from stockdata.dbconnection import getCsvDirectory

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


def formatFn(fn, format):
    fn = f'{getCsvDirectory()}/{fn}'
    fn = os.path.splitext(fn)[0]
    fmat = '.csv' if format.lower() == 'csv' else '.json'
    d = dt.datetime.now()
    fn = f'{fn}_{d.strftime("%Y%m%d_%H%M%S")}{fmat}'
    return fn


def formatData(df, format):
    if format == 'json':
        if df.empty:
            return ''
        return df.to_json()
    elif format == 'visualize':
        if df.empty:
            return ''
        df.sort_values(['time', 'symbol'])
        visualize = []
        for t in df.time.unique():
            tick = df[df.time == t]
            visualize.append({int(t): tick[['symbol', 'price', 'volume']].values.tolist()})
        return json.dumps(visualize)
    elif format == 'csv':
        if df.empty:
            return [[]]
        return df.to_numpy().tolist()


def writeFile(j, fn, format):
    if format in ['json', 'visualize']:
        with open(fn, 'w') as f:
            f.write(j)
    elif format == 'csv':
        with open(fn, 'w', newline='') as f:
            writer = csv.writer(f)
            for row in j:
                writer.writerow(row)


if __name__ == '__main__':
    # assert dt2unix(dt.datetime(2021, 2, 13)) == 1613174400
    # assert dt2unix(dt.datetime(2021, 2, 11)) == 1613001600
    # assert dt2unix(dt.datetime(2021, 2, 11)) != 1613001601
    print(formatFn('fred', 'csv'))
    print(formatFn('fred', 'json'))
