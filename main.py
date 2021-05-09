import pandas as pd
import numpy as np
from gs_client import get_files
from gs_client import download_blob
from tqdm import tqdm
from utils import *
from handle_data import *
import time
from multiprocessing import Pool
from datetime import timedelta
from combineAnalysisSimulation import combineAnalysisSimulation
import os

bucket_name = 'global_data_feed'
#files = [file for file in get_files(bucket_name, 'processed_tick/') if file.endswith('h5')]
#files = [file for file in files if file >= 'processed_tick/20200901.h5']
filedir = "/mnt/disks/gdf/historic_data/data/processed_tick/"
files = sorted([filedir + file for file in os.listdir(filedir)])

class Context:
    def __init__(self, portfolio):
        self.portfolio = portfolio
        
    def set_date(self, date):
        self.todayDate = date
    
    def get_date(self):
        return self.todayDate
    
def saveData(context, df):
    filename1 = "/mnt/disks/gdf/backtesting/analysis/Stock_analyze_log_ltp_"+str(context.get_date()).replace("-", "") + ".csv"
    context.candleDf.to_csv(os.path.abspath(filename1))
    filename2 = "/mnt/disks/gdf/backtesting/order/alice_order_log_"+ str(context.get_date()).replace("-", "") + ".csv"
    log_df = context.alice.get_log(context)
    log_df.to_csv(os.path.abspath(filename2))
    print("Saving Combined Analysis and Simulation")
    combineAnalysisSimulation(filename1, filename2, df, context )

def callHandle1(symbol, ltp, timestamp, qty):
    temp_row = {'symbol': symbol, 'ltp': ltp, 
                'timestamp': timestamp, 'BuyQty': qty}
    handle_data(context,  temp_row)
n = None
def callHandle(symbol, ltp, timestamp, qty, df):
    # 14 seconds with overhead, 1.5 second without overhead
    temp_row = {'symbol': symbol, 'ltp': ltp, 
                'timestamp': timestamp, 'BuyQty': qty}
    # 40 seconds with overhead
    rejected = isRejected(context, symbol)
    # 2 seonds without overhead
    context.alice.set_transaction(temp_row)
    if not rejected and temp_row['timestamp'].hour == 15:
        context.alice.square_off_all(context)
    elif (not rejected) or context.candleDf.at[temp_row['symbol'], '15MinHigh'] is None:
        context.alice.process_orders(context)
        handle_data(context,  temp_row, df)

def process_ticks(df):
    df = df.reset_index(drop = True)
    #print(df.head())
    symbol = df.symbol[0]
    context.candleDf.at[symbol, 'todayOpen'] = df.ltp[0]
    # Before 15 min
    min15_end = datetime.strptime(str(context.get_date()) + ' ' +  '09:30:00', '%Y-%m-%d %H:%M:%S')
    df_15min = df[df.timestamp < min15_end]
    at_min15_logic(context, symbol, df_15min)
    if isRejected(context, symbol): return
    # After 15 min
    df1 = df[df.timestamp >= min15_end]
    temp = [callHandle(symbol, ltp, timestamp, qty, df) for (symbol, ltp, timestamp, qty)\
            in zip(df1['symbol'], df1['ltp'], df1['timestamp'], df1['BuyQty'])]

context = Context(100000)
initialize(context)
for file in (files[16:]):
    #download_blob(bucket_name, file, 'temp_tick.h5')
    print(file)
    df = pd.read_hdf(file)
    df = df.loc[df.symbol.isin(context.symbols)].reset_index(drop=True)
    print("len = ", len(df))
    df['timestamp'] = pd.to_datetime(df['timestamp']) +  pd.to_timedelta((df.index%99999).astype(int), 'us')
    df['date'] = df['timestamp'].dt.date
    df['time'] = df['timestamp'].dt.time
    df['highTillNow'] = df.groupby(['symbol'])['ltp'].transform(lambda x: x.cummax(axis = 0))
    df['lowTillNow'] = df.groupby(['symbol'])['ltp'].transform(lambda x: x.cummin(axis = 0))
    df['diff'] = df['highTillNow'] - df['lowTillNow']
    date = df.loc[0, 'date']
    print(date)
    context.set_date(date)
    day_init(context)
    n = len(df)
    start_time = time.time()
    df_groups = [x for _, x in df.groupby('symbol')]
    for group in tqdm(df_groups):
        process_ticks(group)
    #with Pool(4) as pool:
    #    temp = pool.map(process_ticks, df_groups)
    end_time = time.time()
    print("T = ", end_time - start_time)
    saveData(context, df)


