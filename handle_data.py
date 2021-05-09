# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 15:45:50 2021

@author: Dell
"""
import pandas as pd
import numpy as np
from utils import at_min15_logic, at_breakout_logic, at_breakdown_logic, round_x, setReject, isRejected
from order_logic import bullish_order_logic, bearish_order_logic
import os

def handle_data(context, curr_data, df):
    symbol, price, timestamp = curr_data['symbol'], curr_data['ltp'], curr_data['timestamp']
    #______After 15 minute: Bullish Stock________
    if context.candleDf.at[symbol, 'class'] == "Bullish":
        if not context.candleDf.at[symbol, 'breakout']:
            #______If breakout happens_______
            if curr_data['ltp'] > context.candleDf.at[symbol, '15MinHigh']:
                at_breakout_logic(context, curr_data, df)
                bullish_order_logic(context, curr_data)
            elif curr_data['ltp'] < context.candleDf.at[symbol, '15MinLow']:
                setReject(context, symbol,  'Bullish-Breakdown')
        #______After breakout_________
        else:
            bullish_order_logic(context, curr_data)
    #________After 15 minute: Bearish Stock________        
    elif context.candleDf.at[symbol, 'class'] == "Bearish":
        if not context.candleDf.at[symbol, 'breakdown']:
            #________If breakdown happens_________
            if curr_data['ltp'] < context.candleDf.at[symbol, '15MinLow']:
                at_breakdown_logic(context, curr_data, df)
                bearish_order_logic(context, curr_data)
            elif curr_data['ltp'] > context.candleDf.at[symbol, '15MinHigh']:
                setReject(context, symbol, 'Bearish-Breakout')
        #________After breakdown_________
        else:
            bearish_order_logic(context, curr_data)
