# -*- coding: utf-8 -*-
"""
Created on Thu Feb 25 20:54:49 2021

@author: Dell
"""
from alice_blue_simulation import Alice, TransactionType, ProductType, OrderType

def createAliceBlueObject(portfolio):
    alice = Alice(portfolio)
    return alice

def buy(alice, symbol, quantity, capPrice, stopPrice, targetPrice, entry_type):
    print("alice blue buy")
    alice.place_order(transaction_type = TransactionType.Buy,
                     symbol = alice.get_instrument_by_symbol('NSE', symbol),
                     quantity = quantity,
                     order_type = OrderType.Limit,
                     entry_type = entry_type,
                     product_type = ProductType.BracketOrder,
                     capPrice = capPrice,
                     trigger_price = None,
                     stop_loss = stopPrice,
                     square_off = targetPrice,
                     trailing_sl = None,
                     is_amo = False)
    
def sell(alice, symbol, quantity, capPrice, stopPrice, targetPrice, entry_type):
    print("alice blue sell")
    alice.place_order(transaction_type = TransactionType.Sell,
                     symbol = alice.get_instrument_by_symbol('NSE', symbol),
                     quantity = quantity,
                     order_type = OrderType.Limit,
                     entry_type = entry_type,
                     product_type = ProductType.BracketOrder,
                     capPrice = capPrice,
                     trigger_price = None,
                     stop_loss = stopPrice,
                     square_off = targetPrice,
                     trailing_sl = None,
                     is_amo = False)
    
