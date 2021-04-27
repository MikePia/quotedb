# import time
import datetime as dt
import os
import json
import re
import pandas as pd
from quotedb.dbconnection import getCsvDirectory
from quotedb.utils import util
from quotedb.finnhub.finntrade_ws import ProcessData
from quotedb.models.common import createFirstQuote


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
                    if interval != ms:
                        print("skipped between", old, ts)
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


def runviz():
    procd = ProcessData([], None, dt.timedelta(seconds=10))
    dirnm = os.path.join(getCsvDirectory(), 'json2')
    fq = util.dt2unix_ny(dt.datetime(2021, 4, 22, 9, 30))
    # fq = createFirstQuote(fq, AllquotesModel, )
    for fn in procd.getKeepAlives(dirnm):
        print(fn)
        procd.visualizeData(fn, fq)


if __name__ == '__main__':
    # dirnm = 'data'
    # assert os.path.exists(dirnm)
    # # filenmpattern = "^report"
    # ms = 250
    # # testfile(dirnm, filenmpattern, ms)
    # filenmpat = "^report_"
    # analyzeResults(getCsvDirectory(),  filenmpat)
    ###################################################
    runviz()

    xxx = """asef
        1 3.1648358
        2 5.9464797 2.7816439
        3 23.3807777 17.434298
        4 65.1331792 41.752401500000005
        5 65.1891515 0.055972299999993425

        1 65.1907577
        2 70.436611 5.245853299999993
        3 87.6259513 17.189340299999998
        4 167.4209471 79.79499580000001
        5 167.4793728 0.05842569999998659
        
        1 167.4805372
        2 169.5801671 2.099629900000025
        3 186.7315034 17.151336299999997
        4 217.2222051 30.49070169999999
        5 217.2888156 0.06661049999999591
        1 217.2904089
        2 219.5170153 2.2266064000000085
        3 236.51482 16.99780469999999
        4 267.1805628 30.665742800000032
        5 267.2270859 0.046523100000001705
        1 267.2282358
        2 269.368811 2.1405752000000007
        3 286.5631869 17.19437590000001
        4 316.7813474 30.21816050000001
        5 316.8339036 0.05255619999996952
        1 316.8356039
        2 318.4075552 1.5719512999999665
        3 335.7769409 17.36938570000001
        4 359.5244414 23.7475005
        5 359.5802788 0.05583739999997306
        1 359.5816759
        2 360.9371194 1.3554434999999785
        3 378.3359447 17.398825300000055
        4 393.2782269 14.942282199999966
        5 393.3353285 0.05710160000000997

        1 393.3364171
        2 394.7320211 1.3956039999999916
        3 412.0338775 17.30185640000002
        4 431.2214172 19.187539700000002
        5 431.2780509 0.05663369999996348

        1 431.2792043
        2 432.5215589 1.2423545999999988
        3 449.7026455 17.181086600000015
        4 466.2965608 16.593915299999992
        5 466.3478489 0.0512880999999652
        c:/python/E/uw/quotedb/data\json2\accumulate_55_json_20210425_172156.json  
        1 466.3489395
        2 467.6536675 1.3047280000000114
        3 484.7073086 17.053641099999993
        4 502.2989634 17.591654800000015
        5 502.3562707 0.057307299999990846
        c:/python/E/uw/quotedb/data\json2\accumulate_5_json_20210425_163422.json   
        1 502.3573385
        2 504.6119221 2.2545835999999895
        3 521.9584566 17.34653449999996
        4 552.6074918 30.64903520000007
        5 552.6566498 0.04915799999992032
        c:/python/E/uw/quotedb/data\json2\accumulate_60_json_20210425_172642.json  
        1 552.6577288
        2 556.5397919 3.882063099999982
        3 573.3111594 16.771367499999997
        4 622.3306234 49.019464000000085
        5 622.3824677 0.051844299999970644
        c:/python/E/uw/quotedb/data\json2\accumulate_65_json_20210425_173127.json  
        1 622.3835554
        2 624.0646537 1.6810983000000306
        3 641.0533678 16.98871410000004
        4 665.3328012 24.279433399999903
        5 665.3855842 0.052783000000090396
        c:/python/E/uw/quotedb/data\json2\accumulate_70_json_20210425_173613.json  
        1 665.3872134
        2 666.3575004 0.9702870000000985
        3 683.5727589 17.215258500000004
        4 696.6516721 13.078913199999988
        5 696.7074655 0.05579339999997046
        c:/python/E/uw/quotedb/data\json2\accumulate_75_json_20210425_174058.json
        1 696.7085972
        2 697.6288064 0.9202092000000448
        3 715.0288491 17.40004269999997
        4 725.6159082 10.587059100000033
        5 725.6680171 0.05210890000000745
        """