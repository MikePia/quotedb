# On scheduling the quotes
* A user of a web app (presumable an admin) can schedule the quotes to start and stop.
    * This could mean the result of scheduling a job is a **cron** ar **at** job. In which case the python code is responsible for stopping, but has no power on starting
    * Or the program runs continusously as a daemon off the start. Basically that just means creating a task scheduler in python to start the task and the runquote loop is still responsible for ending it.
    * Or use a tool that nicely  puts it to sleep for a time and runs continusously. If this is the chosen route, it will also need a method to restart/recover
    
