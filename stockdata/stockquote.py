import requests
import time
import datetime as dt

class InvalidServerResponseException(Exception):
    pass

class StockQuote:
    BASEQUOTE = "https://finnhub.io/api/v1/quote/us?"

    def runquote(self):
        base = 
        # params = {"token": "c0b4p7748v6sc0gs4oq0"}
        response = requests.get(self.BASEQUOTE, headers=HEADERS)
        status = response.status_code
        return response.json()


    # TODO: use Twitsted or Chronus
    # https://stackoverflow.com/questions/474528/what-is-the-best-way-to-repeatedly-execute-a-function-every-x-seconds
    def getQuotes(self, start:datetime.datetime, stop:datetime.datetime, freq:float):
        starttime = time.time()
        while start >= starttime:
            json = runquote()
            print (len)
            time.sleep(freq - ((time.time() - starttime)))
            if time.time() stop:
                break


def runit():
    start = dt.datetime.now() + dt.deltatime(seconds=90)
    stop = dt.datetime.now() + dt.deltatime(seconds=190)
    freq = dt.deltatime(seconde=15)

    sq = StockQuote()
    sq.getQuotes(start, stop, freq)
    
if __name__ == '__main__':
    runit()
