import numpy as np
import pandas as pd
import time
import datetime
#from datetime import date, timedelta
from nasdaqfetcher import fetchStocks
from gcpsentiment import analyzeSentiments
from google.gax.errors import RetryError

symbolsDict = {
	"Apple": "AAPL",
	"Google": "GOOG",
	"Microsoft": "MSFT",
	"Facebook": "FB",
	"Amazon": "AMZN",
	"Intel": "INTC",
	"Comcast": "CMCSA",
	"Cisco": "CSCO",
	"Kraft": "KHC",
	"Priceline": "PCLN",
	"Netflix": "NFLX", 
	"Starbucks": "SBUX",
	"PayPal": "PYPL",
	"Adobe": "ADBE", 
	"Tesla": "TSLA",
	"T-Mobile": "TMUS",
	"Fox": "FOX",
	"eBay": "EBAY",
	"JD": "JD",
	"Monster": "MNST",
	"Lam": "LRCX", 
	"Ross": "ROST",
	"American Airlines": "AAL",
	"Symantec": "SYMC"
}
aliasDict = {
	"Google": "Alphabet"
}
START_DATE = datetime.date(2014, 3, 10)
END_DATE = datetime.date(2014, 8, 10)

def getStocks(company):
	if company in stocksDict:
		return stocksDict[company]
	symbol = symbolsDict[company]
	startDate, endDate = START_DATE.strftime("%Y%m%d"), END_DATE.strftime("%Y%m%d")
	stocksDict[company] = fetchStocks(company, startDate, endDate)
	return stocksDict[company]

def getScoreCount(company, date):
	if company in scoreCountDict:
		if date in scoreCountDict[company]:
			return scoreCountDict[company][date]
	else:
		scoreCountDict[company] = {}
	headlines = []
	if date in headlinesDict:
		for headline in headlinesDict[date]:
			if company in headline or company in aliasDict and aliasDict[company] in headline:
				headlines.append(headline)
	if len(headlines) == 0:
		scoreCountDict[company][date] = None
	else:
		next(qc)
		try:
			score, count = analyzeSentiments(headlines), len(headlines)
			scoreCountDict[company][date] = (score, count)
		except RetryError as re:
			print(re.cause.exception())
			scoreCountDict[company][date] = None
	return scoreCountDict[company][date]

def quotaChecker():
	start = time.time()
	counter = 0
	while True:
		cur = time.time()
		if cur - start >= 100:
			start = cur
			counter = 0
			yield
		elif counter >= 500:
			while cur - start < 200:
				cur = time.time()
			yield
		else:
			yield

def getSCAbsDelta(sc1, sc2):
	adScore = sc2[0] - sc1[0]
	adCount = sc2[1] - sc1[1]
	return adScore, adCount

def getSCPerDelta(sc1, sc2):
	pdScore = float(sc2[0])/sc1[0] - 1.0
	pdCount = float(sc2[1])/sc1[1] - 1.0
	return pdScore, pdCount

def getStockAbsDelta(stock1, stock2):
	return stock2 - stock1

def getStockPerDelta(stock1, stock2):
	return float(stock2)/stock1 - 1.0

def getStocksDataframe(company):
	stocksdf = pd.DataFrame(columns=['date',
		'score', 'scoreabsdelta', 'scoreperdelta',
		'count', 'countabsdelta', 'countperdelta',
		'stock', 'stockabsdelta', 'stockperdelta'])
	stocks = getStocks(company)
	prevsc = None
	prevstock = None
	for date in stocks.keys():
		sc = getScoreCount(company, date)
		if sc is None:
			continue
		if prevsc is None and prevstock is None:
			prevsc = sc
			prevstock = stocks[date]
		scoreabsdelta, countabsdelta = getSCAbsDelta(prevsc, sc)
		scoreperdelta, countperdelta = getSCPerDelta(prevsc, sc)
		stockabsdelta = getStockAbsDelta(prevstock, stocks[date])
		stockperdelta = getStockPerDelta(prevstock, stocks[date])
		row = {'date':date,
		'score':sc[0], 'scoreabsdelta':scoreabsdelta, 'scoreperdelta':scoreperdelta,
		'count':sc[1], 'countabsdelta':countabsdelta, 'countperdelta':countperdelta,
		'stock':stocks[date], 'stockabsdelta':stockabsdelta, 'stockperdelta':stockperdelta}
		stocksdf.append(row, ignore_index=True)
	return stocksdf

if __name__ == "__main__":
	headlines = pd.read_csv('uci-news-aggregator.csv')[['TITLE', 'TIMESTAMP']]
	headlinesDict = {}
	for index, row in headlines.iterrows():
		headline, timestamp = row['TITLE'], row['TIMESTAMP']
		date = datetime.date.fromtimestamp(timestamp//1000).isoformat()
		if date not in headlinesDict:
			headlinesDict[date] = [headline]
		else:
			headlinesDict[date].append(headline)
	del headlines

	stocksDict = {}
	scoreCountDict = {}
	parentChildDict = {}
	qc = quotaChecker()
	for company in symbolsDict.keys():
		stocksdf = getStocksDataframe(company)
		stocksdf.to_csv(company + ".csv")
	print('Done')
