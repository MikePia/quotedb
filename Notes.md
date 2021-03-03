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
 * Current decision is ot leave the data in the database and 
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
    * I had hoped that working on a team would fix that but I have seen no indication of any kind of collaboration.
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

##### Alternative is to use polygon
* Preliminary. trade endpoint looks promising
* There is a websocket interce, The cdocs are not helpful, going to have to figure it out
* The trade quote has a lot of what is needed. One day of SQ retrieves more than 200k quotes between 2AM till 11PM
    * If we store it we should probably resample to 1 a second (?)

