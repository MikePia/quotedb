"""
A mock implemntation of a scheduler. The interface should be the same. Must implement run(func, args)
"""


import datetime as dt
import time

class Scheduler():
    mock_schedule_data = {}
    def __init__(self):
        n = dt.datetime.now()
        self.mock_schedule_data = [
            {'start': n + dt.timedelta(seconds=30) , 'stop': n + dt.timedelta(seconds=230), 'freq': 10},
            {'start': n + dt.timedelta(seconds=260) , 'stop': n + dt.timedelta(seconds=270), 'freq': 0.9}
        ]
    
    def run(self, meth, args):
        for job in self.mock_schedule_data:
            n = dt.datetime.now()
            while n < job.start:
                sleeptime = (job.start - n).totalseconds()

    def addJob(self, start, stop)

