import csv
import datetime as dt
import json
import os

import pandas as pd
from quotedb.dbconnection import getCsvDirectory

EPOC = dt.datetime(1970, 1, 1)
MODE = None


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
    df[col] = df[col].apply(
        lambda ts: dt.datetime.fromtimestamp(ts / div))

    # make the datetimes into an index
    df.set_index(col, inplace=True)

    # resample to desired period
    df = df.resample(rule).asfreq().reset_index()

    # convert datetimes back to epoch
    epoch = dt.datetime.fromtimestamp(0)
    df[col] = df[col].apply(
        lambda ts: (ts - epoch).total_seconds())
    return df


def formatFn(fn, format):
    fn = f'{getCsvDirectory()}/{fn}'
    fn = os.path.splitext(fn)[0]
    fmat = '.csv' if format.lower() == 'csv' else '.json'
    d = dt.datetime.now()
    fn = f'{fn}_{d.strftime("%Y%m%d_%H%M%S")}{fmat}'
    return fn


previousTimestamps = pd.DataFrame()


def formatData(df, store, fill=False):
    """
    Paramaters
    ----------
    :params df: DataFrame. If store includes visualize, must include the collumns ['timestamp', 'stock']
    :params store: list.
    """
    global previousTimestamps
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

            visualize.append({json.dumps(int(int(t) / 1000000)): tick[cols].to_json(orient="records")})
        return json.dumps(visualize, separators=(',', ':')).replace('"[', '[').replace(']"', ']').replace("\\", "")[1:-1]

    elif 'json' in store:
        if df.empty:
            return ''
        return df.to_json()
    elif 'csv' in store:
        if df.empty:
            return [[]]
        return df.to_csv(header=True)


def _bracketdata(fn, content):
    with open(fn, 'w') as f:
        f.write('[' + content + ']')


def _replaceLastBracket(fn, newcontent):
    '''
    This relies on the file having no extra spaces at the end.
    We rely on being the only editor of the file to sort of guarantee it.
    '''
    cursize = os.path.getsize(fn)
    with open(fn, 'r+') as f:
        begchar = f.readline().strip()[0]
        f.seek(cursize-1)
        lastchar = f.read().strip()[-1]
        if begchar == '[' and lastchar == ']':
            f.seek(cursize-1)
            overwrite = findDups2(fn, newcontent)
            if not overwrite:
                newcontent = ',' + newcontent + ']'
            else:
                newcontent = overwrite
            f.write(newcontent)
        else:
            raise ValueError('File is in bad state, nothing appended')


def writeFile(j, fn, store):
    global MODE
    if MODE:
        mode = MODE
    else:
        mode = 'a' if (os.path.exists(fn) and os.path.getsize(fn) > 0) else 'w'
    if 'visualize' in store or 'json' in store:

        if mode == 'a':
            _replaceLastBracket(fn, j)
        else:
            if MODE:
                with open(fn, 'w') as f:
                    f.write(j)
            _bracketdata(fn, j)
    elif 'csv' in 'store':
        with open(fn, mode, newline='') as f:
            writer = csv.writer(f)
            for row in j:
                writer.writerow(row)
    MODE = None


def findDups2(fn,  j):
    global MODE
    MODE = None
    with open(fn, 'r+') as f:
        content = f.read()
    try:
        fjson = json.loads(content)
        fjson2 = json.loads("[" + j + "]")
    except Exception:
        return
    dups = {}
    fixthese = []

    for x in fjson:
        ts = list(x.keys())[0]
        if dups.get(ts):
            dups[ts].append(x[ts])
            print('duplicate', ts)
            fixthese.append(ts)
        else:
            dups[ts] = [x[ts]]

    for x in fjson2:
        ts = list(x.keys())[0]
        if dups.get(ts):
            dups[ts].append(x[ts])
            print('duplicate in new stuff', ts)
            fixthese.append(ts)
        else:
            dups[ts] = [x[ts]]

    d3final = {}
    if fixthese:
        MODE = 'w'
        for x in fixthese:
            stocks = [z['stock'] for z in dups[x][0]]
            d3final[x] = []
            for stock in stocks:
                d3 = {}
                d1 = [z for z in dups[x][0] if z['stock'] == stock][0]
                d2 = [z for z in dups[x][1] if z['stock'] == stock][0]
                d3['stock'] = stock
                d3['price'] = (d1['price'] * (d1['volume'] + 1) + (d2['price'] * (d2['volume'] + 1))) / (d1['volume'] + d2['volume'] + 2)
                d3['volume'] = d1['volume'] + d2['volume']
                d3['delta_p'] = (d1['delta_p'] + d2['delta_p']) / 2

                #  TODO. Something not right -- with fq and this delta_v formula is not right- will be close enough for testing
                d3['delta_v'] = (d1['delta_v'] + d2['delta_v']) / 2
                d3['delta_t'] = x
                d3final[x].append(d3)
            dups[x] = d3final[x]
        return json.dumps([dups])
    return None


def getPrevTuesWed(td):
    '''
    Explanation
    -----------
    Utility method to get a probable market open day prior to td. Created for
    unittest.
    :params td: A Datetime object
    '''
    deltdays = 7
    if td.weekday() < 2:
        deltdays = 5 - td.weekday()
    elif td.weekday() < 3:
        deltdays = 0
    elif td.weekday() < 5:
        deltdays = 2
    else:
        deltdays = 4
    before = td - dt.timedelta(deltdays)
    return before


if __name__ == '__main__':
    # assert dt2unix(dt.datetime(2021, 2, 13)) == 1613174400
    # assert dt2unix(dt.datetime(2021, 2, 11)) == 1613001600
    # assert dt2unix(dt.datetime(2021, 2, 11)) != 1613001601
    print(unix2date(1613001600))
    print(unix2date_ny(1613001600))
