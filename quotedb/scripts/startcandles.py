#!/usr/bin/env python
"""
Run this using the runscript command with arguments presented as a dictionary
and the keys -s, -m -d stocks, model and date
"""
import logging
import os
import argparse
import sys
import pandas as pd
from quotedb import sp500
# from quotedb.utils import util
import pandas as pd
from quotedb.getdata import startCandles
from quotedb.models.candlesmodel import CandlesModel
from quotedb.models.allquotes_candlemodel import AllquotesModel
# from quotedb.models.topquotes_candlemodel import TopquotesModel
from quotedb.utils import util 

p = argparse.ArgumentParser()
p.add_argument('-s', '--stocks', type=str, nargs='?', help="""
stocks values may be one of [all, s&p500, nasdaq100, s&p_q100]
or it may be a quoted list of stocks like "APPL MSFT FRED" """)
p.add_argument('-m', '--model', type=str, required=True)
p.add_argument('-d', '--date', type=str, required=True)
p.add_argument('-n', '--numcycles', type=int, default=0)
p.add_argument('-l', '--latest',  default=False, action="store_true")
args = p.parse_args()
print(args)
stockargs = ['all', 's&p500',  'nasdaq100',  's&p_q100']

if args.stocks:
    if args.stocks not in stockargs:
        stocks = args.stocks.split()
    elif args.stocks == 'all':
        stocks = sp500.getSymbols()
    elif args.stocks == 's&p500':
        stocks = sp500.sp500symbols
    elif args.stocks == 'nasdaq100':
        stocks = sp500.nasdaq100symbols
    elif args.stocks == "s&p_q100":
        stocks = sp500.getQ100_Sp500()

if args.model:
    if args.model == "candles":
        model = CandlesModel
    elif args.model == "allquotes":
        model = AllquotesModel
    else:
        raise ValueError("unrecognized model {args.model")
if args.date:
    try:
        start = pd.Timestamp(args.date)
    except Exception:
        print(f"Date format not recognized: {args.date}")
        sys.exit()
    start = util.dt2unix_ny(start)


if not os.environ.get('RUNDIR'):
    from dotenv import load_dotenv
    path = os.path.normpath(os.path.join(__file__, '..'))
    load_dotenv()
    if not os.environ.get('RUNDIR'):
        logging.error('Failed to load the working environment')
        sys.exit()

fn = os.path.join(os.environ.get('RUNDIR'), os.path.splitext(os.path.split(__file__)[1])[0] + ".pid")
with open(fn, 'w') as f:
    f.write(str(os.getpid()))


print(os.getpid())
startCandles(stocks, start, model, latest=True, numcycles=args.numcycles)
