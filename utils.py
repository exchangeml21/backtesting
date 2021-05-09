# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 15:51:54 2021

@author: Dell
"""

import pandas as pd
import  numpy as np
from tag_classifier import get_class
import requests
from datetime import timedelta
from datetime import datetime
#from tickerstore.store import TickerStore
from alice_blue_logic import createAliceBlueObject
import math
import configvals as cfg

# Return True if both entry1 and entry2 rejected.
def isRejected(context, symbol):
    return context.candleDf.at[symbol, 'entry1Reject'] and \
            context.candleDf.at[symbol, 'entry2Reject']

def setReject(context, symbol, rejectType):
    context.candleDf.at[symbol, ['entry1Reject', 'entry2Reject']] = True
    context.candleDf.at[symbol, ['entry1RejectType', 'entry2RejectType']] = rejectType
    
def round_x(x):
    if np.isnan(x): return x
    a = 0.05
    return round(math.ceil(x / a)*a, 2)

def getCandleOHLC(context, symbol, df):
    origin_str = str(context.get_date()) + ' 09:15:00'
    # origin = origin_str,
    df = df.groupby([pd.Grouper(freq='300S', origin=origin_str, 
                                    closed='left', label='left',
                                    level=0)])['ltp'].agg(
                                    open =  "first",
                                    high =  "max",
                                    low =   "min",
                                    close = "last")
    df['ohlc'] = df.values.tolist()
    context.candleDf.at[symbol, 'firstFiveMinuteOhlc'] = tuple(df.loc[df.index[0], 'ohlc'])
    context.candleDf.at[symbol, 'secondFiveMinuteOhlc'] = tuple(df.loc[df.index[1], 'ohlc'])
    context.candleDf.at[symbol, 'thirdFiveMinuteOhlc'] = tuple(df.loc[df.index[2], 'ohlc'])

# Called just after the 15 minute is completed
def at_min15_logic(context, symbol, min15_df):
    df = min15_df
    df = df.set_index(['timestamp'], drop = True)
    context.candleDf.at[symbol, '15MinHigh'] = np.max(df['ltp'])
    context.candleDf.at[symbol, '15MinHighIndex'] = df['ltp'].idxmax()
    context.candleDf.at[symbol, '15MinLow'] = np.min(df['ltp'])
    context.candleDf.at[symbol, '15MinLowIndex'] = df['ltp'].idxmin()
    getCandleOHLC(context, symbol, df)    
    score, _class, rulesScore = get_class(context, symbol)
    context.candleDf.at[symbol, 'score'] = score
    context.candleDf.at[symbol, 'class'] = _class
    context.candleDf.at[symbol, 'rulesScore'] = rulesScore
    diff = context.candleDf.at[symbol, '15MinHigh'] -\
            context.candleDf.at[symbol, '15MinLow']
    if diff > cfg.min15Volatility_alpha*context.candleDf.at[symbol, 'atr']:
        setReject(context, symbol,  '15Min-Volatility-Exceeded')
    elif _class == "Neutral":
        setReject(context, symbol, "Neutral-Stock")

#Called when breakout happens
def at_breakout_logic(context, curr_row, df):
    symbol, timestamp, price = curr_row['symbol'], curr_row['timestamp'], curr_row['ltp']
    context.candleDf.at[symbol, 'breakout'] = True
    context.candleDf.at[symbol, 'entry1Timestamp'] = timestamp
    fibLow = np.min(df.loc[(df.timestamp > context.candleDf.at[symbol, '15MinHighIndex']) &\
                            (df.timestamp < timestamp), 'ltp'])
    marginPercent = context.candleDf.at[symbol, 'marginPercent']
                                
    min15Low = context.candleDf.at[symbol, '15MinLow']
    min15High = context.candleDf.at[symbol, '15MinHigh']
    prevDayClose = context.candleDf.at[symbol, 'prevDayClose']
    atr = context.candleDf.at[symbol, 'atr']
    Z = min15High - fibLow
    X = min15High - prevDayClose
    retracement = Z / X

    context.candleDf.at[symbol, 'Z'] = Z
    context.candleDf.at[symbol, 'X'] = X
    context.candleDf.at[symbol, 'retracement'] = retracement
    context.candleDf.at[symbol, 'fibLow'] = fibLow
    targetPrice = round_x(min(min15Low, prevDayClose) + cfg.target_alpha * atr)
    stopPrice = round_x(min(min15Low, prevDayClose) - cfg.stopLoss_alpha * atr)

    if targetPrice <= min15High:
        context.candleDf.at[symbol, 'entry1RejectType'] = "0.7ATR-Exceeded"
        context.candleDf.at[symbol, 'entry2RejectType'] = "0.7ATR-Exceeded"
        context.candleDf.at[symbol, 'entry1Reject'] = True
        context.candleDf.at[symbol, 'entry2Reject'] = True
        return

    r = None
    if retracement <= 0.382:  #Strong Bull
        context.candleDf.at[symbol, 'subClass'] = "StrongBull"
        r = 0.382
    elif retracement <= 0.618:   #Weak Bull
        context.candleDf.at[symbol, 'subClass'] = "WeakBull"
        r = 0.618
    else:
        context.candleDf.at[symbol, 'entry1RejectType'] = "Retracement-Exceeded"
        context.candleDf.at[symbol, 'entry2RejectType'] = "Retracement-Exceeded"
        context.candleDf.at[symbol, 'entry1Reject'] = True
        context.candleDf.at[symbol, 'entry2Reject'] = True
        return
    
    #_______30% of Stock________
    entry1Loss, entry1Profit = abs(price - stopPrice), abs(targetPrice - price)
    context.candleDf.at[symbol, 'entry1Loss'] = entry1Loss
    context.candleDf.at[symbol, 'entry1Profit'] = entry1Profit
    capEntry1 = round_x(cfg.capEntry1_alpha*(targetPrice - stopPrice) + stopPrice)
    riskToReward = None
    try:
        riskToReward = entry1Loss / entry1Profit
        context.candleDf.at[symbol, 'entry1RiskToReward'] = round_x(riskToReward)
        if abs(riskToReward) <=cfg.riskToRewardEntry1_alpha: 
            qty = (0.3*context.chunkPortfolio)//(capEntry1*marginPercent)
            order = Order(symbol, qty, price, capEntry1, 
                          targetPrice, stopPrice, timestamp, "entry1")
            context.pendingOrders[symbol]['entry1'] = order
            context.candleDf.at[symbol, 'entry1Quantity'] = qty
        else:
            context.candleDf.at[symbol, 'entry1RejectType'] = "RRThreshold-Exceeded"
            context.candleDf.at[symbol, 'entry1Reject'] = True
    except ZeroDivisionError:
        context.candleDf.at[symbol, 'entry1Reject'] = True
        context.candleDf.at[symbol, 'entry1RejectType'] = "Zero-Profit"
    
    #_______70% of Stock________
    limitPrice = round_x(min15High - r * X)
    entry2Loss, entry2Profit = abs(limitPrice - stopPrice), abs(targetPrice - limitPrice)
    context.candleDf.at[symbol, 'entry2Loss'] = entry2Loss
    context.candleDf.at[symbol, 'entry2Profit'] = entry2Profit
    capEntry2 = round_x(cfg.capEntry2_alpha*(targetPrice - stopPrice) + stopPrice)
    riskToReward = None
    try:
        riskToReward = entry2Loss / entry2Profit
        context.candleDf.at[symbol, 'entry2RiskToReward'] = round_x(riskToReward)
        if abs(riskToReward) <=cfg.riskToRewardEntry2_alpha:
            qty = (0.7*context.chunkPortfolio)//(capEntry2*marginPercent)
            order = Order(symbol, qty, limitPrice, capEntry2,
                          targetPrice, stopPrice, timestamp, "entry2")
            context.pendingOrders[symbol]['entry2'] = order
            context.candleDf.at[symbol, 'entry2Quantity'] = qty
        else:
            context.candleDf.at[symbol, 'entry2RejectType'] = "RRThreshold-Exceeded"
            context.candleDf.at[symbol, 'entry2Reject'] = True
    except ZeroDivisionError:
        context.candleDf.at[symbol, 'entry2Reject'] = True
        context.candleDf.at[symbol, 'entry2RejectType'] = "Zero-Profit"
        
    #_______Save in Df________
    context.candleDf.at[symbol, 'entry1Price'] = price
    context.candleDf.at[symbol, 'entry2Price'] = limitPrice
    context.candleDf.at[symbol, 'target'] = targetPrice
    context.candleDf.at[symbol, 'stopPrice'] = stopPrice
    context.candleDf.at[symbol, 'capEntry1'] = capEntry1
    context.candleDf.at[symbol, 'capEntry2'] = capEntry2
    
#Called when breakdown happens
def at_breakdown_logic(context, curr_row, df):
    symbol, timestamp, price = curr_row['symbol'], curr_row['timestamp'], curr_row['ltp']
    context.candleDf.at[symbol, 'breakdown'] = True
    context.candleDf.at[symbol, 'entry1Timestamp'] = timestamp
    marginPercent = context.candleDf.at[symbol, 'marginPercent']
    fibHigh = np.max(df.loc[(df.timestamp > context.candleDf.at[symbol, '15MinLowIndex']) &\
                            (df.timestamp < timestamp), 'ltp'])  
    min15High = context.candleDf.at[symbol, '15MinHigh']
    min15Low = context.candleDf.at[symbol, '15MinLow']
    prevDayClose = context.candleDf.at[symbol, 'prevDayClose']
    atr = context.candleDf.at[symbol, 'atr']
    Z = fibHigh - min15Low
    X = prevDayClose - min15Low
    retracement = Z / X
    context.candleDf.at[symbol, 'Z'] = Z
    context.candleDf.at[symbol, 'X'] = X
    context.candleDf.at[symbol, 'retracement'] = retracement
    context.candleDf.at[symbol, 'fibHigh'] = fibHigh
    targetPrice = round_x(max(min15High, prevDayClose) - cfg.target_alpha * atr)
    stopPrice = round_x(max(min15High, prevDayClose) + cfg.stopLoss_alpha * atr)
    
    if targetPrice >= min15Low:
        context.candleDf.at[symbol, 'entry1RejectType'] = "0.7ATR-Exceeded"
        context.candleDf.at[symbol, 'entry2RejectType'] = "0.7ATR-Exceeded"
        context.candleDf.at[symbol, 'entry1Reject'] = True
        context.candleDf.at[symbol, 'entry2Reject'] = True
        return
    
    #Strong Bear
    r = None
    if retracement <= 0.382:
        context.candleDf.at[symbol, 'subClass'] = "StrongBear"
        r = 0.382
    elif retracement <= 0.618:
        context.candleDf.at[symbol, 'subClass'] = "WeakBear"
        r = 0.618
    else:
        context.candleDf.at[symbol, 'entry1RejectType'] = "Retracement-Exceeded"
        context.candleDf.at[symbol, 'entry2RejectType'] = "Retracement-Exceeded"
        context.candleDf.at[symbol, 'entry1Reject'] = True
        context.candleDf.at[symbol, 'entry2Reject'] = True
        return

    #________30% of Stock_________
    entry1Loss, entry1Profit = abs(stopPrice - price), abs(price - targetPrice)
    context.candleDf.at[symbol, 'entry1Loss'] = entry1Loss
    context.candleDf.at[symbol, 'entry1Profit'] = entry1Profit
    capEntry1 = round_x(cfg.capEntry1_alpha*(targetPrice - stopPrice) + stopPrice)
    try:
        riskToReward = entry1Loss / entry1Profit
        context.candleDf.at[symbol, 'entry1RiskToReward'] = riskToReward
        if abs(riskToReward) <=cfg.riskToRewardEntry1_alpha: 
            qty = qty = (0.3*context.chunkPortfolio)//(capEntry1*marginPercent)
            order = Order(symbol, qty, price, capEntry1, 
                          targetPrice, stopPrice, timestamp, "entry1")
            context.pendingOrders[symbol]['entry1'] = order
            context.candleDf.at[symbol, 'entry1Quantity'] = qty
        else:
            context.candleDf.at[symbol, 'entry1RejectType'] = "RRThreshold-Exceeded"
            context.candleDf.at[symbol, 'entry1Reject'] = True
    except ZeroDivisionError:
        context.candleDf.at[symbol, 'entry1Reject'] = True
        context.candleDf.at[symbol, 'entry1RejectType'] = "Zero-Profit"
    
    #________70% of Stock_________
    limitPrice = round_x(min15Low + r * X)
    entry2Loss, entry2Profit = abs(stopPrice - limitPrice), abs(limitPrice - targetPrice)
    context.candleDf.at[symbol, 'entry2Loss'] = entry2Loss
    context.candleDf.at[symbol, 'entry2Profit'] = entry2Profit
    capEntry2 = round_x(cfg.capEntry2_alpha*(targetPrice - stopPrice) + stopPrice)
    try:
        riskToReward = entry2Loss / entry2Profit
        context.candleDf.at[symbol, 'entry2RiskToReward'] = riskToReward
        if abs(riskToReward) <=cfg.riskToRewardEntry2_alpha:
            qty = (0.7*context.chunkPortfolio)//(capEntry2*marginPercent)
            order = Order(symbol, qty, limitPrice, capEntry2,
                          targetPrice, stopPrice, timestamp, "entry2")
            context.pendingOrders[symbol]['entry2'] = order
            context.candleDf.at[symbol, 'entry2Quantity'] = qty
        else:
            context.candleDf.at[symbol, 'entry2RejectType'] = "RRThreshold-Exceeded"
            context.candleDf.at[symbol, 'entry2Reject'] = True
    except ZeroDivisionError:
        context.candleDf.at[symbol, 'entry2Reject'] = True
        context.candleDf.at[symbol, 'entry2RejectType'] = "Zero-Profit"
        
    #_______Save in Df________
    context.candleDf.at[symbol, 'entry1Price'] = price
    context.candleDf.at[symbol, 'entry2Price'] = limitPrice
    context.candleDf.at[symbol, 'target'] = targetPrice
    context.candleDf.at[symbol, 'stopPrice'] = stopPrice
    context.candleDf.at[symbol, 'capEntry1'] = capEntry1
    context.candleDf.at[symbol, 'capEntry2'] = capEntry2
    
def getATR(hist_df):
    tr_list = []
    prevDayClose = hist_df.loc[hist_df.index[0], 'c']
    for _,(_, opening, high, low, closing, v, oi) in hist_df.loc[hist_df.index[1:]].iterrows():
        tr = max([high - low, abs(high - prevDayClose), abs(low - prevDayClose)])
        tr_list.append(tr)
        prevDayClose = closing
    return np.mean(tr_list)

def initialize(context):
    context.marginUrl = 'https://www1.nseindia.com/archives/nsccl/var/C_VAR1_17022021_1.DAT'
    context.symbols = ['ACC', 'ADANIENT', 'ADANIPORTS', 'AMARAJABAT', 'AMBUJACEM',
       'APOLLOHOSP', 'APOLLOTYRE', 'ASHOKLEY', 'ASIANPAINT', 'AUROPHARMA',
       'AXISBANK', 'BAJAJFINSV', 'BAJFINANCE', 'BANDHANBNK', 'BANKBARODA',
       'BATAINDIA', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BHEL',
       'BIOCON', 'BPCL', 'CADILAHC', 'CANBK', 'CHOLAFIN', 'CIPLA',
       'COALINDIA', 'COFORGE', 'COLPAL', 'CONCOR', 'DABUR', 'DIVISLAB',
       'DLF', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK',
       'GAIL', 'GLENMARK', 'GMRINFRA', 'GODREJCP', 'GRASIM', 'HAVELLS',
       'HCLTECH', 'HDFCBANK', 'HEROMOTOCO', 'HINDALCO', 'HINDPETRO',
       'HINDUNILVR', 'ICICIBANK', 'IDEA', 'IDFCFIRSTB', 'IGL', 'INDIGO',
       'INDUSINDBK', 'INFY', 'IOC', 'JINDALSTEL', 'JSWSTEEL', 'JUBLFOOD',
       'KOTAKBANK', 'LT', 'LUPIN', 'MANAPPURAM', 'MFSL', 'MINDTREE',
       'MRF', 'NAUKRI', 'NESTLEIND', 'NMDC', 'ONGC', 'PAGEIND', 'PEL',
       'PETRONET', 'PIDILITIND', 'PNB', 'POWERGRID', 'RAMCOCEM',
       'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBILIFE', 'SBIN',
       'SHREECEM', 'SUNPHARMA', 'SUNTV', 'TATACHEM', 'TATAMOTORS',
       'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TVSMOTOR',
       'UBL', 'ULTRACEMCO', 'VEDL', 'VOLTAS', 'WIPRO', 'ZEEL']
    context.symbols = ['BHARATFORG'] 
    context.symbols = ['ACC', 'ADANIENT', 'ADANIPORTS', 'AMARAJABAT', 'AMBUJACEM',
       'APOLLOHOSP', 'APOLLOTYRE', 'ASHOKLEY', 'ASIANPAINT', 'AUROPHARMA',
       'AXISBANK', 'BAJAJFINSV', 'BAJFINANCE', 'BANDHANBNK', 'BANKBARODA',
       'BATAINDIA', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BHEL',
       'BIOCON', 'BPCL', 'CADILAHC', 'CANBK', 'CHOLAFIN', 'CIPLA',
       'COALINDIA',  'COLPAL', 'CONCOR', 'DABUR', 'DIVISLAB',
       'DLF', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK',
       'GAIL', 'GLENMARK', 'GMRINFRA', 'GODREJCP', 'GRASIM', 'HAVELLS',
       'HCLTECH', 'HDFCBANK', 'HEROMOTOCO', 'HINDALCO', 'HINDPETRO',
       'HINDUNILVR', 'ICICIBANK', 'IDEA', 'IDFCFIRSTB', 'IGL', 'INDIGO',
       'INDUSINDBK', 'INFY', 'IOC', 'JINDALSTEL', 'JSWSTEEL', 'JUBLFOOD',
       'KOTAKBANK', 'LT', 'LUPIN', 'MANAPPURAM', 'MFSL', 'MINDTREE',
       'MRF', 'NAUKRI', 'NESTLEIND', 'NMDC', 'ONGC', 'PAGEIND', 'PEL',
       'PETRONET', 'PIDILITIND', 'PNB', 'POWERGRID', 'RAMCOCEM',
       'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBILIFE', 'SBIN',
       'SHREECEM', 'SUNPHARMA', 'SUNTV', 'TATACHEM', 'TATAMOTORS',
       'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TVSMOTOR',
       'UBL', 'ULTRACEMCO', 'VEDL', 'VOLTAS', 'WIPRO', 'ZEEL']
    context.symbols = ['ACC', 'ADANIENT', 'ADANIPORTS', 'UBL']
    isinMappingDf = pd.read_csv("isin_mapping.csv", index_col = False, usecols=['SYMBOL', 'ISIN'])
    isinMappingDf = isinMappingDf.rename(columns = lambda x: x.lower())
    context.isinMappingDf = isinMappingDf.drop_duplicates(subset = ["symbol"]).set_index(['symbol'])
    context.chunkPortfolio = context.portfolio / 10.0
    print("Chunk Portfolio = {}".format(context.chunkPortfolio))
    df = pd.read_csv("pc_atr.csv")
    df['date'] = [datetime.strptime(date, '%Y-%m-%d').date() for date in df['date']]
    context.atrDf = df.set_index(['symbol', 'date'])
    print(context.atrDf.head())
    print(context.atrDf.index[0])
def day_init(context):
    context.alice = createAliceBlueObject(context.portfolio)
    date_str = "".join(reversed(str(context.get_date()).split("-")))
    #temp = open('nse_margin.csv', 'wb').write(r.content)
    nseMarginDf = pd.read_csv("nse_margin.csv", skiprows=[0], index_col = False, 
                              usecols=[2, 3, 9], names = ['tag', 'isin', 'marginPercent'])\
                    .set_index(['isin'])
    nseMarginDf = nseMarginDf.drop(nseMarginDf[nseMarginDf['tag'] != 'EQ'].index)
    
    context.pendingOrders = {}
    context.transactionData = []
    context.candleDf = pd.DataFrame(
                            columns = ['symbol', 'prevDayClose', 'todayOpen', 'atr',
                            'firstFiveMinuteOhlc', 'secondFiveMinuteOhlc', 'thirdFiveMinuteOhlc', 
                            '15MinHigh', '15MinHighIndex', '15MinLow', '15MinLowIndex',
                            'fibLow', 'fibHigh', 'retracement', 'breakout', 'breakdown', 
                            'rulesScore', 'score', 'class', 'subClass', 'highTillNow', 'lowTillNow', 
                            'Z', 'X', 'entry1Reject', 'entry1RejectType', 'entry1Price', 'entry1Timestamp', 
                            'capEntry1', 'entry1Profit',  'entry1Loss', 'entry1Quantity', 'entry1RiskToReward', 
                            'entry2Reject', 'entry2RejectType', 'entry2Price', 'entry2Timestamp', 'capEntry2',
                            'entry2Profit', 'entry2Loss', 'entry2Quantity', 'entry2RiskToReward',
                            'target', 'stopPrice', 'marginPercent',
                            'entry1Ordered', 'entry2Ordered'])
    context.candleDf = context.candleDf.set_index(['symbol'])
    dateFrom = context.get_date() - timedelta(days = 30)
    dateTo = context.get_date() - timedelta(days = 1)
    for symbol in context.symbols:
        context.candleDf.at[symbol, ['breakout', 'breakdown']] = False
        context.candleDf.at[symbol, ['entry1Reject', 'entry2Reject']] = False
    
        context.candleDf.at[symbol, 'score'] = 0.0
        context.candleDf.at[symbol, 'highTillNow'] = -1e10
        context.candleDf.at[symbol, 'lowTillNow'] = 1e10
        context.candleDf.at[symbol, '15MinHigh'] = None
        context.candleDf.at[symbol, 'todayOpen'] = None
        context.pendingOrders[symbol] = {}
        context.candleDf.at[symbol, ['firstFiveMinuteOhlc', 'secondFiveMinuteOhlc', 
                                     'thirdFiveMinuteOhlc']] = [(), (), ()]
        context.candleDf.at[symbol, 'marginPercent'] =\
                nseMarginDf.loc[context.isinMappingDf.loc[symbol, 'isin']].marginPercent / 100.0
        date = pd.Timestamp(context.get_date())
        context.candleDf.at[symbol, 'prevDayClose'] = context.atrDf.loc[(symbol, date), 'pc']
        context.candleDf.at[symbol, 'atr'] = context.atrDf.loc[(symbol, date), 'atr']


from order_logic import Order
