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

