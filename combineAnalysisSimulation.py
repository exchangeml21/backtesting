#!/usr/bin/env python
# coding: utf-8

# In[313]:
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import csv
import os
from datetime import datetime
import numpy as np
from utils import round_x

def combineAll():
    analysisFiles = ["/mnt/disks/gdf/backtesting/analysis/" + t for t in sorted(os.listdir("/mnt/disks/gdf/backtesting/analysis"))]
    orderFiles = ["/mnt/disks/gdf/backtesting/order/" + t for t in sorted(os.listdir("/mnt/disks/gdf/backtesting/order"))]
    for file1, file2 in zip(analysisFiles, orderFiles):
        combineAnalysisSimulation(file1, file2)

def combineAnalysisSimulation(file_analysis, file_simulation):
    df1 = pd.read_csv(file_simulation)

    df1['id']=df1['symbol'].astype('str')+df1['entryPosition'].astype('str')+df1['buyOrSell'].astype('str')
    g=df1.groupby('id')
    df1['EntryTradePrice'] = df1.quantity  * df1.tradePrice / g.quantity.transform("sum")
    df1['EntryTradePrice'] = [round_x(x) for x in g.EntryTradePrice.transform("sum")]
    df1['quantity'] = g.quantity.transform("sum")
    df1['portfolioChange']=g.portfolioChange.transform("sum").round(3)
    df1['id']=df1['id'].astype('str')
    df1=df1.drop_duplicates(subset =['id'],keep = 'first')
    df1=df1.loc[:, ['id', 'symbol','entryPosition','stockType','tradeTime',
                    'quantity','buyOrSell','portfolioChange','endOfTheDay','EntryTradePrice']]

    df_exit = df1[(df1['stockType'].str.match('Bearish') & df1['buyOrSell'].str.match('B')) |\
                    (df1['stockType'].str.match('Bullish') & df1['buyOrSell'].str.match('S'))]\
                    .rename(columns = {'EntryTradePrice': 'ExitTradePrice'})
    df_exit = df_exit.assign(id = df_exit.symbol + df_exit.entryPosition)
                    
    df_entry = df1[(df1['stockType'].str.match('Bearish') & df1['buyOrSell'].str.match('S')) |\
                    (df1['stockType'].str.match('Bullish') & df1['buyOrSell'].str.match('B'))]
    df_entry = df_entry.assign(id = df_entry.symbol + df_entry.entryPosition)

    df_inner = pd.merge(df_entry, df_exit,\
                        left_on=['id','symbol','entryPosition','stockType','quantity'],\
                        right_on=['id','symbol','entryPosition','stockType','quantity'],\
                        how='outer')
    df_inner = df_inner.assign(portfolioChange = df_inner.portfolioChange_x + df_inner.portfolioChange_y)\
                        .drop(columns = ['endOfTheDay_x', 'buyOrSell_y', 'buyOrSell_x', 
                                        'portfolioChange_x', 'portfolioChange_y'])

    temp=df_inner[(df_inner['stockType'].str.match('Bearish') )]
    temp['Profit/Loss']=(temp['EntryTradePrice']-temp['ExitTradePrice'])*temp['quantity']
    temp1=df_inner[(df_inner['stockType'].str.match('Bullish') )]
    temp1['Profit/Loss']=(temp1['ExitTradePrice']-temp1['EntryTradePrice'])*temp1['quantity']

    tempo = temp.merge(temp1, on=['id','symbol','entryPosition','stockType','tradeTime_x',
                                    'quantity','EntryTradePrice','tradeTime_y','endOfTheDay_y',
                                    'ExitTradePrice','portfolioChange','Profit/Loss'], how = 'outer')
    tempo['endOfTheDay_y'] = tempo.endOfTheDay_y.astype(str)
    temp=tempo[(tempo['endOfTheDay_y'].str.match('True') ) & (tempo['stockType'].str.match('Bearish'))]
    temp=temp[temp['Profit/Loss']<0]
    temp['Exit Type']='Partial-Loss-Bear'

    temp1=tempo[(tempo['endOfTheDay_y'].str.match('True') ) & (tempo['stockType'].str.match('Bearish'))]
    temp1=temp1[temp1['Profit/Loss']>0]
    temp1['Exit Type']='Partial-Profit-Bear'

    temp2=tempo[(tempo['endOfTheDay_y'].str.match('True') ) & (tempo['stockType'].str.match('Bullish'))]
    temp2=temp2[temp2['Profit/Loss']>0]
    temp2['Exit Type']='Partial-Profit-Bull'

    temp3=tempo[(tempo['endOfTheDay_y'].str.match('True') ) & (tempo['stockType'].str.match('Bullish'))]
    temp3=temp3[temp3['Profit/Loss']<0]
    temp3['Exit Type']='Partial-Loss-Bull'

    merge_cols = list(temp.columns.intersection(temp1.columns))
    sam = temp.merge(temp1, on = merge_cols, how = 'outer')\
              .merge(temp2, on = merge_cols, how = 'outer')\
              .merge(temp3, on = merge_cols, how = 'outer')

    temp4=tempo[(tempo['endOfTheDay_y'].str.match('False') ) & (tempo['stockType'].str.match('Bearish'))]
    temp4=temp4[temp4['Profit/Loss']<0]
    temp4['Exit Type']='Stop-Loss-Bear'
    temp5=tempo[(tempo['endOfTheDay_y'].str.match('False') ) & (tempo['stockType'].str.match('Bearish'))]
    temp5=temp5[temp5['Profit/Loss']>0]
    temp5['Exit Type']='Target-Profit-Bear'

    temp6=tempo[(tempo['endOfTheDay_y'].str.match('False') ) & (tempo['stockType'].str.match('Bullish'))]
    temp6=temp6[temp6['Profit/Loss']>0]
    temp6['Exit Type']='Target-Profit-Bull'

    temp7=tempo[(tempo['endOfTheDay_y'].str.match('False') ) & (tempo['stockType'].str.match('Bullish'))]
    temp7=temp7[temp7['Profit/Loss']<0]
    temp7['Exit Type']='Stop-Loss-Bull'
    
    merge_cols = list(temp4.columns.intersection(temp5.columns))
    final = temp4.merge(temp5, on = merge_cols, how = 'outer')\
                 .merge(temp6, on = merge_cols, how = 'outer')\
                 .merge(temp7, on = merge_cols, how = 'outer')\
                 .merge(sam, on = merge_cols, how = 'outer')

    data=pd.read_csv(file_analysis, usecols = ['symbol', 'prevDayClose', 'atr'])
    final=final.drop(['id'],axis=1)
    final = pd.merge(final,data, left_on=['symbol'], right_on = ['symbol'], how='inner')

    final = final.rename(columns = {'prevDayClose':'PC','atr':'ATR','tradeTime_x': 'EntryTradeTime', 
                                    'tradeTime_y': 'ExitTradeTime','symbol':'Symbol','quantity':'Quantity',
                                    'entryPosition':'Entry Type','stockType':'Stock View','endOfTheDay_y':'EndOfTheDay',
                                    'ExitTradePrice':'ExitTradePrice','EntryTradePrice':'EntryTradePrice'}, inplace = False)
    final=final.sort_values(by = 'EntryTradeTime')
    final['EntryTradeTime'] = pd.to_datetime(final['EntryTradeTime'])
    final['EntryTradeDate'] = [d.date() for d in final['EntryTradeTime']]
    final['EntryTradeTime'] = [d.time() for d in final['EntryTradeTime']]

    final['ExitTradeTime'] = pd.to_datetime(final['ExitTradeTime'])
    final['ExitTradeDate'] = [d.date() for d in final['ExitTradeTime']]
    final['ExitTradeTime'] = [d.time() for d in final['ExitTradeTime']]

    final=final[['Symbol','Quantity','Stock View','PC','ATR','Entry Type','EntryTradePrice',\
            'EntryTradeTime','EntryTradeDate','ExitTradePrice','ExitTradeTime','ExitTradeDate',\
            'Exit Type','Profit/Loss']]
    if len(final) == 0: return
    filename = "/mnt/disks/gdf/combinedLog/analysis_and_simulation_" + ''.join(str(final['EntryTradeDate'][0]).split('-')) + ".csv"
    print(filename)
    final.to_csv(filename, index = False)

def max_min_f(g, final, context, entryType):
    d = {'BullMaxVal-PostExec': None, 'BullMaxTime-PostExec': None, 
         'BullMinVal-PostExec': None, 'BullMinTime-PostExec': None, 
         'BearMaxVal-PostExec': None, 'BearMaxTime-PostExec': None, 
         'BearMinVal-PostExec': None, 'BearMinTime-PostExec': None, 
         'id': None}
    time_format = "%Y-%m-%d %H:%M:%S.%f"
    pm_3 = datetime.strptime(str(context.get_date()) + ' ' +  '15:00:00.000000', time_format)
    index_0 = g.index[0]
    symbol = g.symbol[index_0]
    _id = symbol + entryType
    d['id'] = _id
    t1 = datetime.strptime(str(context.get_date()) + ' ' + str(final.at[_id, 'EntryTradeTime']), time_format)
    isStopLoss = (final.at[_id, 'Exit Type'].split("-")[0] == "Stop")
    t2 = pm_3
    if isStopLoss:
        t2 = min(t2, datetime.strptime(str(context.get_date()) + ' ' +  str(final.at[_id, 'ExitTradeTime']), time_format))
    t2 = np.min([t2, pm_3])
    target, atr, stopPrice = context.candleDf.loc[symbol, ['target', 'atr', 'stopPrice']]
 
    if final.loc[_id, 'StockView'] == 'Bullish':
        pc, min15Low = context.candleDf.loc[symbol, ['prevDayClose', '15MinLow']]
        ref = min(pc, min15Low)
        ind = g[(g.timestamp > t1) & (g.timestamp < t2)]['ltp'].idxmax()
        d['BullMaxVal-PostExec'] = round((g.at[ind, 'ltp'] - ref) / atr, 3)
        d['BullMaxTime-PostExec'] = g.at[ind, 'timestamp']
        ind = g[(g.timestamp > t1) & (g.timestamp < pm_3)]['ltp'].idxmin()
        d['BullMinVal-PostExec'] = round((g.at[ind, 'ltp'] - ref) / atr, 3)
        d['BullMinTime-PostExec'] = g.at[ind, 'timestamp']
    else:
        pc, min15High = context.candleDf.loc[symbol, ['prevDayClose', '15MinHigh']]
        ref = max(pc, min15High)
        ind = g[(g.timestamp > t1) & (g.timestamp < t2)]['ltp'].idxmin()
        d['BearMinVal-PostExec'] = round((g.at[ind, 'ltp'] - ref) / atr, 3)
        d['BearMinTime-PostExec'] = g.at[ind, 'timestamp']
        ind = g[(g.timestamp > t1) & (g.timestamp < pm_3)]['ltp'].idxmax()
        d['BearMaxVal-PostExec'] = round((g.at[ind, 'ltp'] - ref) / atr, 3)
        d['BearMaxTime-PostExec'] = g.at[ind, 'timestamp']
    return pd.Series(d, index = list(d.keys()))

def combineAnalysisSimulation(file_analysis, file_simulation, df, context):
    df1 = pd.read_csv(file_simulation)

    df1['id']=df1['symbol'].astype('str')+df1['entryPosition'].astype('str')+df1['buyOrSell'].astype('str')
    g=df1.groupby('id')
    df1['EntryTradePrice'] = df1.quantity  * df1.tradePrice / g.quantity.transform("sum")
    df1['EntryTradePrice'] = [round_x(x) for x in g.EntryTradePrice.transform("sum")]
    df1['quantity'] = g.quantity.transform("sum")
    df1['portfolioChange']= g.portfolioChange.transform("sum").round(3)
    df1['id']=df1['id'].astype('str')
    df1=df1.drop_duplicates(subset =['id'],keep = 'first')
    df1=df1.loc[:, ['id', 'symbol','entryPosition','stockType','tradeTime',
                    'quantity','buyOrSell','portfolioChange','endOfTheDay','EntryTradePrice']]

    df_exit = df1[(df1['stockType'].str.match('Bearish') & df1['buyOrSell'].str.match('B')) |\
                    (df1['stockType'].str.match('Bullish') & df1['buyOrSell'].str.match('S'))]\
                    .rename(columns = {'EntryTradePrice': 'ExitTradePrice'})
    df_exit = df_exit.assign(id = df_exit.symbol + df_exit.entryPosition)
                    
    df_entry = df1[(df1['stockType'].str.match('Bearish') & df1['buyOrSell'].str.match('S')) |\
                    (df1['stockType'].str.match('Bullish') & df1['buyOrSell'].str.match('B'))]
    df_entry = df_entry.assign(id = df_entry.symbol + df_entry.entryPosition)

    df_inner = pd.merge(df_entry, df_exit,\
                        left_on=['id','symbol','entryPosition','stockType','quantity'],\
                        right_on=['id','symbol','entryPosition','stockType','quantity'],\
                        how='outer')
    df_inner = df_inner.assign(portfolioChange = df_inner.portfolioChange_x + df_inner.portfolioChange_y)\
                        .drop(columns = ['endOfTheDay_x', 'buyOrSell_y', 'buyOrSell_x', 
                                        'portfolioChange_x', 'portfolioChange_y'])

    temp=df_inner[(df_inner['stockType'].str.match('Bearish') )]
    temp['Profit/Loss']=(temp['EntryTradePrice']-temp['ExitTradePrice'])*temp['quantity']
    temp1=df_inner[(df_inner['stockType'].str.match('Bullish') )]
    temp1['Profit/Loss']=(temp1['ExitTradePrice']-temp1['EntryTradePrice'])*temp1['quantity']

    tempo = temp.merge(temp1, on=['id','symbol','entryPosition','stockType','tradeTime_x',
                                    'quantity','EntryTradePrice','tradeTime_y','endOfTheDay_y',
                                    'ExitTradePrice','portfolioChange','Profit/Loss'], how = 'outer')
    tempo['endOfTheDay_y'] = tempo.endOfTheDay_y.astype(str)
    temp=tempo[(tempo['endOfTheDay_y'].str.match('True') ) & (tempo['stockType'].str.match('Bearish'))]
    temp=temp[temp['Profit/Loss']<0]
    temp['Exit Type']='Partial-Loss-Bear'

    temp1=tempo[(tempo['endOfTheDay_y'].str.match('True') ) & (tempo['stockType'].str.match('Bearish'))]
    temp1=temp1[temp1['Profit/Loss']>0]
    temp1['Exit Type']='Partial-Profit-Bear'

    temp2=tempo[(tempo['endOfTheDay_y'].str.match('True') ) & (tempo['stockType'].str.match('Bullish'))]
    temp2=temp2[temp2['Profit/Loss']>0]
    temp2['Exit Type']='Partial-Profit-Bull'

    temp3=tempo[(tempo['endOfTheDay_y'].str.match('True') ) & (tempo['stockType'].str.match('Bullish'))]
    temp3=temp3[temp3['Profit/Loss']<0]
    temp3['Exit Type']='Partial-Loss-Bull'

    merge_cols = list(temp.columns.intersection(temp1.columns))
    sam = temp.merge(temp1, on = merge_cols, how = 'outer')\
              .merge(temp2, on = merge_cols, how = 'outer')\
              .merge(temp3, on = merge_cols, how = 'outer')

    temp4=tempo[(tempo['endOfTheDay_y'].str.match('False') ) & (tempo['stockType'].str.match('Bearish'))]
    temp4=temp4[temp4['Profit/Loss']<0]
    temp4['Exit Type']='Stop-Loss-Bear'
    temp5=tempo[(tempo['endOfTheDay_y'].str.match('False') ) & (tempo['stockType'].str.match('Bearish'))]
    temp5=temp5[temp5['Profit/Loss']>0]
    temp5['Exit Type']='Target-Profit-Bear'

    temp6=tempo[(tempo['endOfTheDay_y'].str.match('False') ) & (tempo['stockType'].str.match('Bullish'))]
    temp6=temp6[temp6['Profit/Loss']>0]
    temp6['Exit Type']='Target-Profit-Bull'

    temp7=tempo[(tempo['endOfTheDay_y'].str.match('False') ) & (tempo['stockType'].str.match('Bullish'))]
    temp7=temp7[temp7['Profit/Loss']<0]
    temp7['Exit Type']='Stop-Loss-Bull'
    
    merge_cols = list(temp4.columns.intersection(temp5.columns))
    final = temp4.merge(temp5, on = merge_cols, how = 'outer')\
                 .merge(temp6, on = merge_cols, how = 'outer')\
                 .merge(temp7, on = merge_cols, how = 'outer')\
                 .merge(sam, on = merge_cols, how = 'outer')

    data=pd.read_csv(file_analysis, usecols = ['symbol', 'prevDayClose', 'atr'])
    final = pd.merge(final,data, left_on=['symbol'], right_on = ['symbol'], how='inner')

    final = final.rename(columns = {'prevDayClose':'PC','atr':'ATR','tradeTime_x': 'EntryTradeTime', 
                                    'tradeTime_y': 'ExitTradeTime','symbol':'Symbol','quantity':'Quantity',
                                    'entryPosition':'EntryType','stockType':'StockView','endOfTheDay_y':'EndOfTheDay',
                                    'ExitTradePrice':'ExitTradePrice','EntryTradePrice':'EntryTradePrice'}, inplace = False)
    final=final.sort_values(by = 'EntryTradeTime')
    final['EntryTradeTime'] = pd.to_datetime(final['EntryTradeTime'])
    final['EntryTradeDate'] = [d.date() for d in final['EntryTradeTime']]
    final['EntryTradeTime'] = [d.time() for d in final['EntryTradeTime']]

    final['ExitTradeTime'] = pd.to_datetime(final['ExitTradeTime'])
    final['ExitTradeDate'] = [d.date() for d in final['ExitTradeTime']]
    final['ExitTradeTime'] = [d.time() for d in final['ExitTradeTime']]
    final = final.set_index(['id'], drop = True)
    max_min_df_e1 = df[df.symbol.isin(final[final.EntryType == 'entry1'].Symbol)]\
                        .groupby(['symbol']).apply(lambda x: max_min_f(x, final, context, 'entry1'))
    max_min_df_e2 = df[df.symbol.isin(final[final.EntryType == 'entry2'].Symbol)]\
                        .groupby(['symbol']).apply(lambda x: max_min_f(x, final, context, 'entry2'))
    max_min_df = pd.concat([max_min_df_e1, max_min_df_e2])
    
    cols_to_save = ['Symbol','Quantity','StockView','PC','ATR','EntryType','EntryTradePrice',\
                'EntryTradeTime','EntryTradeDate','ExitTradePrice','ExitTradeTime','ExitTradeDate',\
                'Exit Type','Profit/Loss', 'BullMaxVal-PostExec', 'BullMaxTime-PostExec',\
                'BullMinVal-PostExec', 'BullMinTime-PostExec', 'BearMinVal-PostExec',\
                'BearMinTime-PostExec', 'BearMaxVal-PostExec', 'BearMaxTime-PostExec']
    if len(max_min_df) ==0 or len(final) == 0:
        final = pd.DataFrame(columns = cols_to_save)
    else: 
        final = final.merge(max_min_df, on = ['id']).drop(columns = ['id'])
        final=final[cols_to_save]
    date = file_analysis.split(".")[0][-8:]
    print(date)
    filename = "/mnt/disks/gdf/backtesting/combinedLog/analysis_and_simulation_" + date  + ".csv"
    print(filename)
    final.to_csv(filename, index = False)
