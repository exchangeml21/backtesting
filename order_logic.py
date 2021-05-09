# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 15:44:17 2021

@author: Dell
"""

from alice_blue_logic import sell, buy
from utils import round_x

class Order:
    def __init__(self, symbol, quantity, triggerPrice, capEntry, targetPrice, stopPrice, timestamp, orderType):
        self.symbol = symbol
        self.quantity = int(quantity)
        self.triggerPrice = triggerPrice
        self.capEntry = capEntry
        self.targetPrice = targetPrice
        self.stopPrice = stopPrice
        self.stopLoss = round_x(abs(stopPrice - capEntry))
        self.squareOff = round_x(abs(targetPrice - capEntry))
        self.timestamp = timestamp
        self.orderType = orderType
        
    def print(self):
        print("[symbol:{}, quantity:{}, triggerPrice: {}, capEntry: {}, targetPrice: {}, stopPrice: {}, stopLoss: {}, squareOff: {}, timestamp: {}, orderType:{}]".format(
              self.symbol, self.quantity, self.triggerPrice, self.capEntry,
              self.targetPrice, self.stopPrice, self.stopLoss, self.squareOff, self.timestamp, self.orderType))

# To do 1: Place entry2 order only if current timestamp 
# ...is withing the 30 minute of breakout/breakdown
def bullish_order_logic(context, curr_row):
    symbol = curr_row['symbol']
    price = curr_row['ltp']
    timestamp = curr_row['timestamp']
    if 'entry1' in context.pendingOrders[symbol]:
        order = context.pendingOrders[symbol]['entry1']
        context.candleDf.loc[symbol, 'entry1Timestamp'] = timestamp
        context.candleDf.loc[symbol, 'entry1Ordered'] = True
        print("__BUY__")
        order.print()
        buy(context.alice, order.symbol, order.quantity, 
                order.capEntry, order.stopLoss, order.squareOff, 'entry1')
        del context.pendingOrders[symbol]['entry1']
    
    if 'entry2' in context.pendingOrders[symbol]:
        order = context.pendingOrders[symbol]['entry2']
        diff = timestamp - order.timestamp
        if price <= order.triggerPrice:
            context.candleDf.loc[symbol, 'entry2Timestamp'] = timestamp
            if diff.seconds / 60 <= 30.0:
                context.candleDf.loc[symbol, 'entry2Ordered'] = True
                print("__BUY__")
                order.print()
                buy(context.alice, order.symbol, order.quantity, 
                        order.capEntry, order.stopLoss, order.squareOff, 'entry2')
            else:
                context.candleDf.at[symbol, 'entry2Reject'] = True
                context.candleDf.at[symbol, 'entry2RejectType'] = 'Timeout'
            del context.pendingOrders[symbol]['entry2']

#Order logic for bearish stock
def bearish_order_logic(context, curr_row,):
    symbol = curr_row['symbol']
    price = curr_row['ltp']
    timestamp = curr_row['timestamp']
    if 'entry1' in context.pendingOrders[symbol]:
        order = context.pendingOrders[symbol]['entry1']
        context.candleDf.loc[symbol, 'entry1Timestamp'] = timestamp
        context.candleDf.loc[symbol, 'entry1Ordered'] = True
        print("__SELL__")
        order.print()
        sell(context.alice, order.symbol, order.quantity, 
                 order.capEntry, order.stopLoss, order.squareOff, 'entry1')
        del context.pendingOrders[symbol]['entry1']
    
    if 'entry2' in context.pendingOrders[symbol]:
        order = context.pendingOrders[symbol]['entry2']
        diff = timestamp - order.timestamp
        if price >= order.triggerPrice:
            context.candleDf.loc[symbol, 'entry2Timestamp'] = timestamp
            if diff.seconds / 60 <= 30.0:
                context.candleDf.loc[symbol, 'entry2Ordered'] = True
                print("__SELL__")
                order.print()
                sell(context.alice, order.symbol, order.quantity, 
                         order.capEntry, order.stopLoss, order.squareOff, 'entry2')
            else:
                context.candleDf.at[symbol, 'entry2Reject'] = True     
                context.candleDf.at[symbol, 'entry2RejectType'] = 'Timeout'
            del context.pendingOrders[symbol]['entry2']
