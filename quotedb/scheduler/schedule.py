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
            {'start': n + dt.timedelta(seconds=10), 'stop': n + dt.timedelta(seconds=40), 'freq': 2.5},
            {'start': n + dt.timedelta(seconds=45), 'stop': n + dt.timedelta(seconds=270), 'freq': 0.9}
        ]

    def run(self, meth, arr):
        '''
        Run the series of jobs (currenly just the array mock_schedule_data)
        '''
        for job in arr:
            n = dt.datetime.now()
            if n <= job['start']:
                print('case 1', job)
                sleeptime = (job['start'] - n).total_seconds()
                time.sleep(sleeptime)
                meth(**job)
            elif n < job['stop']:
                print('case 2')
                meth(**job)

    # def addJob(self, start, stop)


def testMe(start=None, stop=None, freq=None):
    n = dt.datetime.now()
    assert n >= start
    while n <= stop:
        one = time.perf_counter()
        print(n.strftime("%A %B %d, %H:%M:%S"))
        print(stop.strftime("%B %A, %H:%M%S"))
        print(freq)
        two = time.perf_counter()
        s = freq - (two - one)
        time.sleep(s if s > 0 else 0)
        n = dt.datetime.now()


if __name__ == '__main__':
    s = Scheduler()
    s.run(testMe, s.mock_schedule_data)
