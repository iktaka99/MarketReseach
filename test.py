from yahooquery import Ticker
import pandas as pd
import datetime

toyota = Ticker('7203.T')
icst = toyota.income_statement()
print(icst)
print(icst.T)
#icst.T.query('index in ("asOfDate", "GrossProfit", "NetIncome", "OperatingIncome", "TotalRevenue")')