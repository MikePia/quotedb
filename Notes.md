# On scheduling the quotes
* A user of a web app (presumable an admin) can schedule the quotes to start and stop.
    * This could mean the result of scheduling a job is a **cron** ar **at** job. In which case the python code is responsible for stopping, but has no power on starting
    * Or the program runs continusously as a daemon off the start. Basically that just means creating a task scheduler in python to start the task and the runquotes loop is still responsible for ending it.
    * Or use a tool that nicely  puts it to sleep for a time and runs continusously. If this is the chosen route, it will also need a method to restart/recover
    
## The SqlAlchemy connection string is  retrieved  using
```python
stockdata.dbconnection.getSaConn()
```
### Not sure the endpoint
"https://finnhub.io/api/v1/quote/us?" is doing what I thought. In the data (that I have not deleted) on the db, there are no changing times:
* select distinct(time) from quotes; has only two results
* Repeated calls repeat the same results
<pre>
+------------+
| time       |
+------------+
| 1613174400 |
| 1613001600 |
+------------+
</pre>
which correspond to 2/13/21 0:0:0 and 2/11/21 0:0:0
On the other hand those are closing times on the last day those stocks were active so maybe on Monday at premarket or open they will stream

## Implementfilling the candle data\
* finnhub candle data is available for 1 year (~13 mo)
* Current obstacle is duplicates
    * Get rid of 90% of problem by finding duplicates before makeing db objects
    * Plan to create a duplicate remover. The problem is that it will break any indexes set on the db

### Query to find duplicates
```sql
 select symbol, time, count(*) from candles group by symbol, time having count(*) > 1;
 ```
 These should be the same
 ```sql
 select count(distinct time) from candles;
 select count(timetime) from candles;
 ```
### Important points of email cc Note this is not stone, just the thinking so-far
#### Three accounts with 3 running queries
* We will have three different accounts that will be used for queries. 
    * The first will cycle through all stocks at 300 calls per minute to be able to change-in-price data for each stock about every 20 minutes. From that data, a list will be created of the top 100 movers. 
    * This list will be cycled through using a second account yielding a quote every 20 seconds. 
    * From that list, the top 10 movers will be flagged and using a third account to generate quotes every 2 seconds. 
* Each quote from each account will be appended to the same table to create a master table of raw data with stocksymbol, time, quote, and volume as columns. 

#### The deltas:
* Using that raw quote data table, a second table will be created to calculate the delta between current and the last two quotes. This will result in columns stocksymbol, timestamp, time_since_last_quote, percent change in price from last quote, percent change in volume since last quote. 

* Lastly, a third table will be created to fill in missing data points. For each stock, if the time interval between two points is greater than 1 seconds, then it will copy the prior quote and create a new row with an artificial quote for the second(s) in between actual quotes. This will be the table used for rendering the visual.  

#### The visual
Jan's proposal includes
* The application will be built to the specifications of a data file that you provide. Subsequent integration with your back-end infrastructure is outside of my scope for this project.
    * So I have no idea how to integrate with his stuff and even less on how he wants to access it. He has not been forthcoming with any signs of collaboration and this bullet point suggests he plans not to collaborate.
    * Maybe this is where Aaron comes in.
* In a previous email I highlighted the gaps in the intraday data being a challenge. I have some sample minute-tick data of my own that I can use to demonstrate what your animated chart will look like.
    * I need to see an example of that data

* provide sample data in .json format, 
    * ??? structured in the same way that it will appear when pulled from your data provider or back-end API. 
    •	Date and time
    •	Ticker
    •	Volume
    •	Price
        * That excludes the quote endpoint as it has no volume
        * Candle endpoint has volume, close would be price
        * This websocket wss://ws.finnhub.io  is the endpoint for Trades. This may be the better candidate
        * [Tick data](https://finnhub.io/docs/api/stock-tick) is maybe possible: XXX ***not realtime data***
            *  https://tick.finnhub.io/  or 'https://finnhub.io/api/v1/stock/tick?
            * symbol is request
            * volume, price, time are in responsees







 ### Milestone 7 Duplicating rows for time gaps
 * Current decision is to leave the data in the database and 
    * generate the rows in the results of an accessor.
* Decisions: 
    * Fill in all times 24/7, 
    * extended 7-19,
    * extended begins with the first data we have and the fills in till the last
    * market 9:30 - 16, 
    * custom time?
    * For single day, simple beg and end
    * For multiple days do the same on each day
    * A filled in row would be use the previous close for each entry
    open:c, high:c, low:c, close:c, volume:0, time:{time}
    * The interface(s) need to cover all possible requests
        * Most recent 5 minutes 
        * a day
        * a specific time in a day
        * multiple days
        * multiple days filled in between (x and y)

#### How to implement
Without the input of how the visual needs the data, I just have to make some sensible decisions. Later, with more data I can change them as long as I implement this as atomically as possible
* getFilledData(self, data, day, begin=9:30, end=16:00 )
    * Get the data from a single day and fill in the gaps
* getDaysofFilledInData(self, data, begin, end, policy=extended)
    * Get the data for the days between begin and end inclusive
    * policy is [extended, market, asis, 247]

### Milestone 8 High Frequency Quotes for fast movers.
* Dilemma Which endpoint. I am thinking the trades websocket endpoint will be the right one
* Bit annoyed that I may have to start over to make this work and it is not fair that I will probably have to absorb the time because I am being paid by Milestone.
    * This is probably not a long term thing unless it moves to a full time kind of thing. DC has not the experience to direct exactly what I need to do but yet pays by completing his instructions.
    * I had hoped that working on a team would fix that. There are some indications with Jans latest report -- phase 2 of his is to integrate.

    * ***The best course of action is to over produce at this point.***
        * Some work is better than no work and I need work
        * The experience is extermely valuable
        * The blips on Upwork are extremely valuable

#### How to implement
* Use the Trade endpoint, websocket interface
    * class MyWebSocket
        * Takes an array of tickers to subscribe data to
        * Saves all to trades table
* Create a new TradeModel to write to db

#### Notes on implementation
The trade endpoint gets current data including tim, price, symbol and volume. 
***But filling in the historical data is a problem***
* trade is current data
* quote doesn't have volume
* quote/us is only end of day
* stock/candle has volume but it's figured differently than trade
* Maybe tick data can fill 
    * It has volume data. I assume it is based on trades at that current tick price.
* ***New version of CycleTrades is working to get historical data***
* Takes an array  of stock a begin date and samplerate 
* Fills in the data and then continuously updates todays data
###### What it is lacking
* A reasonable update that avoids duplicating data
* The results may have alot of 0 volumes (like most entries) from the resample 
    * Ran 5 stocks for about a week of data resamplled .25 seconds, entries with 0 volume at ~97% with about 13 million records
    * Going to implement a removal of data with 0 volume as an option
* A way to fill in data prior to what is already there.
* Bullet proofness
* Need to rework the sqlalchemy sessions to try and create fewer of them.

##### Alternative is to use polygon
* Preliminary. trade endpoint looks promising
* There is a websocket interce, The cdocs are not helpful, going to have to figure it out
* The trade quote has a lot of what is needed. One day of SQ retrieves more than 200k quotes between 2AM till 11PM
    * If we store it we should probably resample to 1 a second (?)

### 3/5/21
### Completed milestone 7 and 8 -- (But neither one is what is currently needed)
* ***State of 8 - fast moving quotes***
    * Using trade endpoint from polygon, ```PolygonApi.cycleStocksToCurrent()```
        * ```PolygonApi(array, begdate, resamprate, filternull, timer=None)```
            * (timer unimplemented)
        * Currently  set to raise exception when Polygon server returns non 200 status and not yet handling the retries
        * Status is stop develpment in favor of tick data -- volume not required
    * Features include
        * Will take and array of stocks any size but only tested with nasdaq100
        * set the day  to start gathering data, will get to current day and continue with real time gathering
        * Set the time to start for current day
        * allow it to gather from the most recent date in the db for the current stock
        * Set the sample rate to aggregate calls (Turning off resampling not enabled but would be simple)
        * Turn on or off to trim the resampled data in which `volume` is 0 (recommended to always trim -- or remove resample to get raw trades)


* ***State of 7 -- Duplicating rows for time gaps***
* Using finnhub candle endpoint from ```ManageCandles.getFilledData()``` or ```ManageCandles.getFilledDataDays()```
    * ```getFilledData(self, symbol, begin, end, format='json')``` 
    * ```getFilledDataDays(self, symbol, startdate, enddate, policy, custom, format)```
        * policy and custom determine the start and end point for each day. For example, policy=market will give data between 9:30-16:00 and ensure each 1 minute entry has a record
* Note that this was the original data and included the report of a years data gathering. Currently there is no use for this call or the candle data. That could obviously change as features are eventually added.

### Nothing but Tick data
* polygon has none and it's web socket keeps tabs on how many subscriptions, I think its three per subscription, and purchase more for $75 so sp 500  so 166*75/month for sp500 simultansous  ***So finnhub it is** (I think)
* More bad news. It seems finnhub tick data is not real time. The paramaters include a date and a skip. ***It is not possible to set a beginning time.*** To geta the days datad, you paginate through the data from skip=0 till data runs out
* ***Au Contraire***--- implement a divide and conquor method to find the first time bit.
    * In the getTicksForADay method, manage the offsets in a dict.
    * The first time a ticker is called (Assuming this is a oneday only thing at this point) call this divide and conqueor method. 
        * This could be potentially time consuming. Reduce the  required calls by using average number between the next pagination and total ```skip = ((skip +len j[t]) + j[total]) //2``` for the next request. 
        * First call use skip = 0, Need to get the total val to figure the second call (iuf necessary)
        * The second call should be able to make a reasonable guess-- better than just divide and conquor based on:
            * len of date
            * diffence between 1st and last time
            * the j[total] value for that day
            * the current time of day
    * ***Don't need all that**
        * Get the first one to find the amount of data, Then hijack the rest of the method and
        get the data in reverse order.  duh
* I*mplement pagination through the day, Then insert the find the date into the top of the loop after the first request.

# Creating apis for the client according to requested specs:
### Creating a single module to create getters for data. 
* Json will be available for all calls
* All results will be ordered by [ticker, symbol]
* Have not implemented 'filling in the holes' on any of these yet
* ***Will probably need to combine our stored data with realtime data***
    * The real time data could be sent straing to the client. The time required to save to our db and then reaccess may be a problem.
    * Problems with that around 300 calls per second. If 1000 clients request new data .
* The module is stockdata.getdata for all the calls listed here. 
    * So getCandles precise address is ```stockdata.getdata.getCandles()```

### candles (currently from Finnhub api, realtime data is availble)

* ```getCandles(stocks:list, start:datetime, end:datetime)```
```
[ { "time": 1611138660,
    "symbol": "PDD",
    "close": 175.55,
    "volume": 276,
    "high": 175.55,
    "low": 175.55,
    "open": 175.55,
    "id": 1771880
    }, ...
```
###  Tick data / Trade data, realtime data from websocket from finnhub
* Finn hub websocket data is realtime websocket, no historical data. Not sure how to use this in my (lack of) understanding of our data model. If we need historical data (even 5 minutes worth), we need to get it somewhere else.
* So why bother? Bercause it's probably the best source of real time data so far.
* So for this first API, it will retrieve any data already in the db for the requested time and trigger the call to start gathering realtime for the stocks in the list. Later, this should probably be done by setting up a websocket server and sending data as requested combining historical and realtime data somehow
* 
```
getTicks(stocks, start, end)
startWS(stocks)
```
(The bit coin data is all I can gather in off hours)
```
'[{
    "price": 47849.9,
    "time": 1614400000000.0,
    "volume": 0.000525,
    "id": 51,
    "condition": null,
    "symbol": "BINANCE:BTCUSDT"
    },
```
### Tick data from Finn hub REST api
* ***Started this after hours on Fri*** and don't know how close this data is to real time
* Focused on getting the last x minutes and (hopefull) continue w realtime data
* The start call requires ***day*** and ***time amount** (e.g. last 30 minutes)
* This endpoint runs out of data at some point -- and ***don't know if the data updates during open hours yet.*** (find out Monday)
 
``` 
getTicksREST(stocks, start, end)
startLastXTicks(stocks, dadate, delt)
```

```
'[{
    "symbol": "CDW",
    "price": 154.61,
    "time_ms": 1614972567024,
    "volume": 1,
    "id": 2643524,
    "condition": "1,8,12"
    },
```
### polygon trade data (realtime)
Including this because this data is both historical and real time. Can give it a date and continue looping with paginations into current trades. So the Api here will include getting the data and then starting collection of historical/current data. 

### Finnhub quote api
* Broke it, needs fixing

### sqlite key storage

### datastructure
#####	Generate sample data that looks basically like this:
* An array of "ticks" where each tick is identified by a timestamp
* for each tick there should be an array of ticker objects, where each object includes a ticker name, the value to be plotted on the y-axis (% change in price), the value to be plotted on the x-axis (perhaps market cap, volume, or change in volume - Don can provide guidance), and optionally a value on which to scale the ticker's size on the chart
* the same list of tickers should be provided for each timestamp (even if there's no change from the prior timestamp)
* data can be provided in either json format or a csv file at this point, but eventually we'll be working with json data sent by the server
