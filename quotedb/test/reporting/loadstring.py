# import time
import os
import json
import re
import pandas as pd
from quotedb.utils import util


def testfile(dirnm, filenmpat, ms):
    os.chdir(dirnm)

    fn = ''
    for x in os.listdir():
        if re.match(filenmpat, x):
            fn = x
            print(x, end='')

            f = open(fn, 'r')
            file = f.read()
            f.close()
            x = file.find('}{')
            assert x == -1

            fjson = json.loads(file)
            assert fjson and len(fjson) > 1
            dups = {}

            old = 0
            for i, x in enumerate(fjson):
                ts = list(x.keys())[0]
                if dups.get(ts):
                    dups[ts].append(x)
                    print('duplicate', ts)
                else:
                    dups[ts] = [x]
                interval = int(ts) - int(old)
                if i > 0:
                    assert interval == ms
                old = ts

            d2 = int(list(fjson[-1].keys())[0])
            d1 = int(list(fjson[0].keys())[0])
            dd1 = util.unix2date(d1, unit='m')
            dd2 = util.unix2date(d2, unit='m')
            print("  ", dd2 - dd1)
            # time.sleep(20)
            # 1618477044250000000


def analyzeResults(dirnm, filenmpat):
    print()
    os.chdir(dirnm)

    fn = ''
    for fn in os.listdir():
        contents = []
        if re.match(filenmpat, fn):
            print(fn, end='')
            f = open(fn, 'r')
            line = ' '
            while line:
                line = f.readline()
                if line:
                    contents.append(json.loads(line))
            f.close()
            contents[0]['begin']
            elapsed = contents[-1]['elapsed'] - contents[0]['begin']
            endcol = pd.Timestamp(contents[-1]['tend'])
            begincol = pd.Timestamp(contents[1]['tbegin'])
            coltime = (endcol - begincol).total_seconds()
            print(f"{elapsed} / {coltime}: {round((coltime / elapsed)*100, 2)}%")


if __name__ == '__main__':
    dirnm = 'data/visualize2'
    assert os.path.exists(dirnm)
    filenmpattern = "^_10sec_"
    ms = 10000
    testfile(dirnm, filenmpattern, ms)
    filenmpat = "^_report_"
    analyzeResults('.',  filenmpat)
