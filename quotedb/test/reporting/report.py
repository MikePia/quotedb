import datetime as dt
import json
import os
import time
from quotedb.utils import util
from quotedb.finnhub.finntrade_ws import ProcessData
import pandas as pd
from aboutfiles import abt

from quotedb.dbconnection import getCsvDirectory
from quotedb import sp500
from quotedb import getdata as gd
from quotedb.models import metamod as mm


# stocks = ['CERN', 'CSCO', 'GILD', 'KDP', 'MAR', 'MU', 'AAPL']
def runit():
    for i in range(4, 76, 5):
        stocks = sp500.random50(numstocks=i)
        # stocks.append('BINANCE:BTCUSDT')
        # delt = dt.timedelta(seconds=10)
        fn = f"accumulate_{len(stocks)}_json.json"
        fn = util.formatFn(fn, 'json')

        # fq = util.dt2unix_ny(dt.datetime(2021, 4, 22, 9, 30))
        # gd.startTickWS_SampleFill(stocks, fn, fq, delt=delt)
        store = ['json']
        gd.startTickWSKeepAlive(stocks, fn, store, delt=None,)
        mm.cleanup()
        mm.init()
        print("sleeping for 30")
        time.sleep(30)


def getfiles(pat):
    fdir = util.getCsvDirectory()
    fdir = os.path.join(fdir, "prod")
    files = os.listdir(fdir)
    fnames = []
    for f in files:
        if f.startswith(pat):
            fnames.append(os.path.join(util.getCsvDirectory(), 'prod/' + f))
    return fnames


def aboutthefiles():
    fnames = getfiles('accumulate')

    outfile = os.path.join(util.getCsvDirectory(), 'prod/aboutfiles.txt')
    out = open(outfile, 'a')
    for fn in fnames:
        # for delt in deltas:
        df = pd.DataFrame()
        with open(fn, 'r') as f:
            out.write(fn + '\n')
            with open(fn, 'r') as f:
                line = ' '
                ix = 0
                while line:
                    line = f.readline()
                    if line:
                        ldf = pd.DataFrame(json.loads(line))
                        if ix == 0:
                            # Verify we got the right data structure
                            assert set(['price', 'stock', 'timestamp', 'volume']) == set(ldf.columns)

                        df = df.append(ldf)
                        ix += 1
                out.write(f'{ix} lines\n')
                df.sort_values(['timestamp'])
                out.write(f"From {util.unix2date(int(df.iloc[0].timestamp), unit='m')} to {util.unix2date(int(df.iloc[-1].timestamp), unit='m')}\n")
                out.write(f'{len(df)}\n\n')
    out.close()


def visualizestuff():
    fnames = getfiles('accumulate')
    fqs = []
    for name, ab in zip(fnames, abt):
        fqs.append(util.dt2unix(ab[2]))

    deltas = [
        dt.timedelta(seconds=0.25),
        dt.timedelta(seconds=1),
        dt.timedelta(seconds=3),
        dt.timedelta(seconds=7),
        dt.timedelta(seconds=10),
    ]
    basefile = 'zavizuell.json'
    for fn, fq in zip(fnames, fqs):

        for delt in deltas:
            proc = ProcessData([], None, delt)
            # outfile = util.formatFn(basefile, 'json')
            proc.visualizeDataNew(fn, fq, basefile, )


def csveeit():
    f1 = [{
        "reading": 0.47213449999999924,
        "indexing": 0.00748770000000043,
        "unduped": 0.3806542999999998,
        "Firstquotecreate": 63.511575099999995,
        "Resample": 2.5597567000000083,
        "filldata": 4.101776700000002,
        "Formatdata": 4.381112000000002,
        "Writefile": 0.005630199999998808
        },
        {
        "reading": 0.5281043999999993,
        "indexing": 0.01398659999999996,
        "unduped": 0.5759205000000005,
        "Firstquotecreate": 61.5466908,
        "Resample": 0.7414138000000037,
        "filldata": 1.0686121999999898,
        "Formatdata": 1.142085900000012,
        "Writefile": 0.0028437999999937347
        },
        {
        "reading": 0.27032510000000043,
        "indexing": 0.003711899999999879,
        "unduped": 0.22856759999999987,
        "Firstquotecreate": 58.2530447,
        "Resample": 0.18844500000000153,
        "filldata": 0.2049076999999997,
        "Formatdata": 0.20612729999999857,
        "Writefile": 0.0024573000000032152
        },
        {
        "reading": 0.23598790000000003,
        "indexing": 0.0033769999999999634,
        "unduped": 0.19502900000000034,
        "Firstquotecreate": 60.715638,
        "Resample": 0.09974859999999808,
        "filldata": 0.08689070000001209,
        "Formatdata": 0.08319599999998673,
        "Writefile": 0.0018848000000133425
        },
        {
        "reading": 0.2149808000000002,
        "indexing": 0.003663799999999995,
        "unduped": 0.19233619999999974,
        "Firstquotecreate": 64.2057806,
        "Resample": 0.10013159999999743,
        "filldata": 0.08337029999999856,
        "Formatdata": 0.09538419999999803,
        "Writefile": 0.0022801000000072236
        },
        {
        "reading": 3.2792337,
        "indexing": 0.029372600000000304,
        "unduped": 3.1965877000000003,
        "Firstquotecreate": 63.1668769,
        "Resample": 6.144468599999996,
        "filldata": 9.765650399999998,
        "Formatdata": 10.517539900000003,
        "Writefile": 0.023807399999995482,
        },
        {
        "reading": 0.9187759999999998,
        "indexing": 0.012345299999999781,
        "unduped": 1.1944237000000006,
        "Firstquotecreate": 62.140133899999995,
        "Resample": 3.2954640000000097,
        "filldata": 6.2669771999999995,
        "Formatdata": 5.354238600000002,
        "Writefile": 0.011669499999996447,
        },
        {
        "reading": 1.0975799000000004,
        "indexing": 0.01165869999999991,
        "unduped": 1.6073968,
        "Firstquotecreate": 62.826074299999995,
        "Resample": 0.8467305000000067,
        "filldata": 1.3535461000000026,
        "Formatdata": 1.3660003999999901,
        "Writefile": 0.0043114000000059605
        },
        {
        "reading": 1.0488656000000005,
        "indexing": 0.011805799999999422,
        "unduped": 1.8007926000000003,
        "Firstquotecreate": 66.7946199,
        "Resample": 0.34909690000000637,
        "filldata": 0.5048680999999959,
        "Formatdata": 0.4862171999999987,
        "Writefile": 0.002683000000004654
        },
        {
        "reading": 0.8754245999999997,
        "indexing": 0.010235100000000052,
        "unduped": 8786500999999998,
        "Firstquotecreate": 62.86352910000001,
        "Resample": 0.25725009999999315,
        "filldata": 0.27211040000000253,
        "Formatdata": 0.19957829999999888,
        "Writefile": 0.002750500000004763
        },
        {
        "reading": 0.9293470999999998,
        "indexing": 0.011786500000000366,
        "unduped": 0.9507199999999996,
        "Firstquotecreate": 63.2319417,
        "Resample": 0.2512449999999973,
        "filldata": 0.2762266000000011,
        "Formatdata": 0.16179610000000366,
        "Writefile": 0.002504599999994639
        },
        {
        "reading": 3.5128464,
        "indexing": 0.030917800000000106,
        "unduped": 4.7992036,
        "Firstquotecreate": 65.47189230000001,
        "Resample": 6.3309302999999915,
        "filldata": 8.858325300000004,
        "Formatdata": 9.656887299999994,
        "Writefile": 0.015314400000008277
        },
        {
        "reading": 2.8863541,
        "indexing": 0.026729800000000026,
        "unduped": 2.6704107000000006,
        "Firstquotecreate": 64.8508408,
        "Resample": 1.8453185000000047,
        "filldata": 2.7553425999999916,
        "Formatdata": 3.068111900000005,
        "Writefile": 0.005794800000003875
        },
        {
        "reading": 3.2742883,
        "indexing": 0.021648800000000357,
        "unduped": 2.983115399999999,
        "Firstquotecreate": 66.35517660000001,
        "Resample": 0.7540726999999947,
        "filldata": 1.1174673000000013,
        "Formatdata": 0.9037847000000028,
        "Writefile": 0.0035132999999945014
        },
        {
        "reading": 2.9001544,
        "indexing": 0.02671150000000022,
        "unduped": 3.298871499999999,
        "Firstquotecreate": 65.22448,
        "Resample": 0.49903740000000596,
        "filldata": 0.47837009999999225,
        "Formatdata": 0.40358650000000296,
        "Writefile": 0.002786600000007411
        },
        {
        "reading": 3.0560819,
        "indexing": 0.029321000000000375,
        "unduped": 2.9451270999999997,
        "Firstquotecreate": 65.8573347,
        "Resample": 0.37369759999999985,
        "filldata": 0.4751887000000039,
        "Formatdata": 0.27656009999999753,
        "Writefile": 0.0026438999999953694
        },
        {
        "reading": 4.816576700000001,
        "indexing": 0.03864139999999949,
        "unduped": 4.5048347,
        "Firstquotecreate": 67.5346554,
        "Resample": 15.017872100000005,
        "filldata": 23.302827599999986,
        "Formatdata": 22.230853100000004,
        "Writefile": 0.03988160000000107
        },
        {
        "reading": 4.6549841999999995,
        "indexing": 0.040458799999999684,
        "unduped": 5.130864300000001,
        "Firstquotecreate": 65.6974857,
        "Resample": 3.6542689999999993,
        "filldata": 5.984439399999999,
        "Formatdata": 5.330798299999998,
        "Writefile": 0.009623599999997623
        },
        {
        "reading": 4.661229400000001,
        "indexing": 0.04930219999999963,
        "unduped": 4.1446054,
        "Firstquotecreate": 65.4938437,
        "Resample": 1.7960445000000078,
        "filldata": 1.8363640999999973,
        "Formatdata": 1.7972886000000017,
        "Writefile": 0.005108800000002134
        },
        {
        "reading": 5.0736713,
        "indexing": 0.048208900000000554,
        "unduped": 4.4213834,
        "Firstquotecreate": 62.5654104,
        "Resample": 0.8823736000000082,
        "filldata": 0.9779946000000024,
        "Formatdata": 0.7135373999999928,
        "Writefile": 0.003099800000001096
        },
        {
        "reading": 4.883599199999999,
        "indexing": 0.031127700000000758,
        "unduped": 4.504354599999999,
        "Firstquotecreate": 63.612115700000004,
        "Resample": 0.7790272999999956,
        "filldata": 0.8346922000000063,
        "Formatdata": 0.6879390999999941,
        "Writefile": 0.002926299999998605,
    }]
    df = pd.DataFrame(f1)
    thecsvreport = df.to_csv()
    with open(os.path.join(getCsvDirectory(), 'speedreport.csv'), 'w', newline='') as f:
        f.write(thecsvreport)


if __name__ == '__main__':
    # runit()
    # aboutthefiles()
    # visualizestuff()
    csveeit()
