# On scheduling the quotes
* A user of a web app (presumable an admin) can schedule the quotes to start and stop.
    * This could mean the result of scheduling a job is a **cron** ar **at** job. In which case the python code is responsible for stopping, but has no power on starting
    * Or the program runs continusously as a daemon off the start. Basically that just means creating a task scheduler in python to start the task and the runquote loop is still responsible for ending it.
    * Or use a tool that nicely  puts it to sleep for a time and runs continusously. If this is the chosen route, it will also need a method to restart/recover
    
## The SqlAlchemy connection string is  retrieved  using
```python
stockdata.stockquote.getSaConn()
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

