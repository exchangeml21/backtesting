# -*- coding: utf-8 -*-
"""
Created on Sat Mar  6 14:05:16 2021

@author: Dell
"""
import enum
from collections import OrderedDict 
import pandas as pd

class TransactionType(enum.Enum):
    Sell = "SELL"
    Buy = "BUY"
    
class OrderType(enum.Enum):
    Limit = "LIMIT"
    
class ProductType(enum.Enum):
    BracketOrder = "BRACKET-ORDER"
    
class OrderStatus(enum.Enum):
    Pending = "PENDING"
    Placed = "PLACED"
    # SquaredOff = "SQUARED-OFF"
    # StoppedLoss = "STOPPED-LOSS"
    Canceled = "CANCELED"
    Exhausted = "EXHAUSTED"
    
class ID:
    def __init__(self):
        self.id = 0
        
    def get_id(self):
        self.id += 1
        return self.id
    
    
class Order:
    def __init__(self, transaction_type, instrument, quantity, order_type, entry_type, product_type, 
                 capPrice, trigger_price, stop_loss, square_off, current_timestamp, 
                 trailing_sl = None, is_amo = False):
        self.transaction_type = transaction_type
        self.symbol = instrument   
        self.quantity = quantity
        self.pendingQuantity = quantity
        self.status = OrderStatus.Pending
        self.placedQueue = []
        self.order_type = order_type
        self.entry_type = entry_type
        self.product_type = product_type
        self.capPrice = capPrice           #capPrice
        self.trigger_price = trigger_price
        self.stop_loss = stop_loss
        self.square_off = square_off
        self.init_timestamp = current_timestamp
        self.trailing_sl = trailing_sl
        self.is_amo = is_amo
        
    def print(self):
        print("Symbol: {}, Quantity: {}, TransactionType: {}, CapPrice:{}, StopLoss: {}, SquareOff: {},  init_timestamp: {}".format(self.symbol, self.quantity, self.transaction_type, self.capPrice, self.stop_loss, self.square_off, self.init_timestamp))
    
class Alice:
    def __init__(self, portfolio):
        self.current_transaction = None
        self.Id = ID()
        self.orders = OrderedDict()
        self.placedOrders = OrderedDict()
        self.origPortfolio = portfolio
        self.portfolio = portfolio
        self.profit = {}
        self.cols = ['symbol', 'entryPosition', 'stockType', 'orderTime', 'tradeTime', 'quantity', 
                    'buyOrSell', 'tradePrice', 'target', 'stopLoss', 'portfolioChange', 'endOfTheDay']
        self.log = []
        
    def get_instrument_by_symbol(self, stock, symbol):
        return symbol
    
    # This is called by the customer
    def place_order(self, transaction_type, symbol, quantity, order_type, entry_type, product_type, 
                    capPrice, trigger_price, stop_loss, square_off, trailing_sl = None, is_amo = False):
        _id = self.Id.get_id()
        order = Order(transaction_type, symbol, quantity, order_type, entry_type, product_type, 
                    capPrice, trigger_price, stop_loss, square_off, self.current_timestamp, 
                    trailing_sl = None, is_amo = False)
        if symbol not in self.orders:
            self.orders[symbol] = {}
            self.profit[symbol] = 0.0
            
        self.orders[symbol][_id] = order
        
    def set_transaction(self, curr_data):
        self.current_transaction = curr_data
        self.volume = 1000000
        self.current_timestamp = curr_data['timestamp']
        
    def append_bullish_in_log(self, context, symbol, order, order_time, qty, placing_price, buyOrSell, endOfTheDay):
        portfolioChange, price = None, self.current_transaction['ltp']
        if buyOrSell == 'B':
            portfolioChange = -qty * price
        else: 
            portfolioChange = qty * price
        targetPrice, stopPrice = placing_price + order.square_off, placing_price - order.stop_loss
        self.log.append(dict(zip(self.cols, [symbol, order.entry_type, "Bullish", order_time, self.current_timestamp, qty, 
                                            buyOrSell, price, targetPrice, stopPrice, portfolioChange, endOfTheDay])))

    def append_bearish_in_log(self, context, symbol, order, order_time, qty, placing_price, buyOrSell, endOfTheDay):
        portfolioChange, price = None, self.current_transaction['ltp']
        if buyOrSell == 'B':
            portfolioChange = -qty * price
        else: 
            portfolioChange = qty * price
        targetPrice, stopPrice = placing_price - order.square_off, placing_price + order.stop_loss
        self.log.append(dict(zip(self.cols, [symbol, order.entry_type, "Bearish", order_time, self.current_timestamp, qty, 
                                            buyOrSell, price, targetPrice, stopPrice, portfolioChange, endOfTheDay])))

    def get_log(self, context):
        print(self.log)
        df = None
        if len(self.log) == 0: 
            df = pd.DataFrame(columns = self.cols)
        else: df = pd.DataFrame(self.log).sort_values(by = ['symbol', 'orderTime'])
        return df

    def square_off_all(self, context):
        symbol = self.current_transaction['symbol']
        price = self.current_transaction['ltp']
        volume = self.volume
        if symbol not in self.orders: 
            return
        for _id, order in self.orders[symbol].items():
            order.status = OrderStatus.Canceled
            if order.transaction_type == TransactionType.Buy:
                for q in order.placedQueue:
                    if q['quantity'] > 0 and volume > 0:
                        qty = min(q['quantity'], volume)
                        self.portfolio += qty * price
                        self.profit[symbol] += qty * (price - q['price'])
                        qty = min(q['quantity'], volume)
                        q['quantity'] -= qty
                        self.append_bullish_in_log(context, symbol, order, q['time'], qty, q['price'], 'S', True)
            else:
                for q in order.placedQueue:
                    if q['quantity'] > 0 and volume > 0:
                        qty = min(q['quantity'], volume)
                        self.portfolio += qty * (q['price'] + (q['price'] - price))
                        self.profit[symbol] += qty * (q['price'] - price)
                        qty = min(q['quantity'], volume)
                        q['quantity'] -= qty
                        self.append_bearish_in_log(context, symbol, order, q['time'], qty, q['price'], 'B', True)
    # This is called for each transaction
    def process_orders(self, context):
        symbol = self.current_transaction['symbol']
        price = self.current_transaction['ltp']
        volume = self.volume
        
        if symbol not in self.orders: return
        # Iterate through all the orders
        for _id, order in self.orders[symbol].items():
            # If customer wants to buy
            if order.transaction_type == TransactionType.Buy:
                # Square off the placed order
                for q in order.placedQueue: 
                    if q['quantity'] > 0 and volume > 0:
                        if (price >= q['price'] + order.square_off or
                            price <= q['price'] - order.stop_loss):
                            qty = min(q['quantity'], volume)
                            volume -= qty
                            q['quantity'] -= qty
                            self.portfolio += qty * price
                            self.profit[symbol] += qty * (price - q['price'])
                            self.append_bullish_in_log(context, symbol, order, q['time'], qty, q['price'], 'S', False)

                # Place the pending order
                if order.status == OrderStatus.Pending and order.pendingQuantity > 0:
                    diff = self.current_timestamp - order.init_timestamp
                    if diff.seconds / 60 > 30.0:
                        order.status = OrderStatus.Canceled
                    elif price <= order.capPrice:
                        qty = min(order.pendingQuantity, volume)
                        if qty > 0:
                            volume -= qty
                            order.pendingQuantity -= qty
                            order.placedQueue.append({'time': self.current_timestamp, 
                                                      'quantity':qty, 
                                                      'status':OrderStatus.Placed, 
                                                      'price':price})
                            self.portfolio -= qty * price
                            self.append_bullish_in_log(context, symbol, order, order.init_timestamp, qty, price, 'B', False)
            #If customer wants to sell
            elif order.transaction_type == TransactionType.Sell:
                # Square off the placed order
                for q in order.placedQueue: 
                    if q['quantity'] > 0 and volume > 0:
                        if (price <= q['price'] - order.square_off or
                            price >= q['price'] + order.stop_loss):
                            qty = min(q['quantity'], volume)
                            volume -= qty
                            q['quantity'] -= qty
                            self.portfolio += qty * (q['price'] + (q['price'] - price))
                            self.profit[symbol] += qty * (q['price'] - price)
                            self.append_bearish_in_log(context, symbol, order, q['time'], qty, q['price'], 'B', False)
                # Place the pending order
                if order.status == OrderStatus.Pending and order.pendingQuantity > 0:
                    diff = self.current_timestamp - order.init_timestamp
                    if diff.seconds / 60 > 30.0:
                        order.status = OrderStatus.Canceled
                    elif price >= order.capPrice:
                        qty = min(order.pendingQuantity, volume)
                        if qty > 0:
                            volume -= qty
                            order.pendingQuantity -= qty
                            order.placedQueue.append({'time': self.current_timestamp,
                                                      'quantity':qty, 
                                                      'status':OrderStatus.Placed, 
                                                      'price': price})
                            self.portfolio -= qty * price
                            self.append_bearish_in_log(context, symbol, order, order.init_timestamp, qty, price, 'S', False)
