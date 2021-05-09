import zipfile
import rarfile
import pandas as pd
import numpy as np
import io
from queue import Queue
from threading import Thread
from multiprocessing import Pool, TimeoutError
import time
import os
from gs_client import upload_blob
import math

start_time = time.time()
symbols = ['ACC', 'ADANIENT', 'ADANIPORTS', 'AMARAJABAT', 'AMBUJACEM',
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
symbols = set(symbols)
tasks = []
print("num_symbols = ", len(symbols))

filedir = "/mnt/disks/gdf/historic_data/data/"
files = sorted([filedir + file for file in os.listdir(filedir) if file.endswith(".zip")])
print(files)
#month_folders = [filedir + x for x in files]

def round_x(x):
    a = 0.05
    return np.round_(np.ceil(x / a)*a, 2)

def concat_and_save(x):
    from gs_client import upload_blob
    (month_folder, date_folder) = x
    print("Start: ",  date_folder)
    month_zipref = zipfile.ZipFile(month_folder, "r")
    with month_zipref.open(date_folder) as z:
        z_filedata = io.BytesIO(z.read())
        lib_obj = zipfile.ZipFile
        if date_folder.endswith(".rar"): 
            date_folder = date_folder.split("_")[-1]
            lib_obj = rarfile.RarFile
  
        with lib_obj(z_filedata, "r") as date_zipref:
            filenames = list(filter(lambda x : x.endswith(".csv"), date_zipref.namelist()))
            df_list = []
            for filename in filenames:
                symbol = filename.split("/")[1].split(".")[-3]
                if symbol in symbols:
                    f = date_zipref.open(filename)
                    df = pd.read_csv(f)
                    df_list.append(df)
            if len(df_list) == 0:
                print("EMPTY")
                return 
            df = pd.concat(df_list)
            df = df[(df['Time']>='09:15:00') &  (df['Time']<='15:30:00')]
            print(df.head(1))
            print(df.tail(1))
            if len(df) == 0:
                print("Zero Len")
                return 
            df = df.reset_index(drop = True)
            df['Date'] = ['-'.join(reversed(x.split("/"))) for x in df['Date']]
            df['timestamp'] = df['Date'] + " " + df['Time']
            df['symbol'] = [x.split(".")[0] for x in df['Ticker']]
            filename = ''.join(df.loc[0, 'Date'].split("-")) + ".h5"
            df.drop(columns = ['Ticker', 'Date', 'Time', 'OpenInterest'], inplace = True)
            df.rename(columns = {'LTP': 'ltp'}, inplace = True)
            convert_dict = {'ltp': np.float32, 'BuyQty': np.uint32,
                            'BuyPrice': np.float32, 'SellQty': np.uint32, 
                            'SellPrice': np.float32, 'symbol': 'str'}
            df = df.astype(convert_dict)
            df['ltp'] = round_x(df['ltp'])
            #df.to_hdf("data/" + filename, key = 'stage', mode = 'w', complevel=1)
            df.to_hdf("/mnt/disks/gdf/historic_data/data/processed_tick/"+filename, key='df',mode='w',complevel=1)
            print(filename)
            local_path = "/mnt/disks/gdf/historic_data/data/processed_tick/"+filename
            upload_blob("global_data_feed", local_path, 'processed_tick/'+ filename)

for month_folder in files:
    print(month_folder)
    month_zipref = zipfile.ZipFile(month_folder, "r")
    date_folders = list(filter(lambda x:x.endswith(".zip") or x.endswith(".rar"), month_zipref.namelist()))
    #date_folders += [x.split("_")[-1] for x in month_zipref.namelist() if x.endswith(".rar")]
    print("date_folders = ", date_folders)
    for date_folder in sorted(date_folders):
        tasks.append((month_folder, date_folder))

for task in tasks:
    concat_and_save(task)

#with Pool(4) as pool:
#    temp = pool.map(concat_and_save, tasks)

end_time = time.time()
print(end_time - start_time)
