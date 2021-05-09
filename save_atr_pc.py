import pandas as pd
import numpy as np
from tqdm import tqdm
import time
import os
import math

def round_x(x):
    if np.isnan(x): return x
    a = 0.05
    return round(math.ceil(x / a)*a, 2)

bucket_name = 'global_data_feed'
#files = [file for file in get_files(bucket_name, 'processed_tick/') if file.endswith('h5')]
#files=[file for file in files if file >= 'processed_tick/20200901.h5']
filedir = "/mnt/disks/gdf/historic_data/data/processed_tick/"
files = sorted([filedir + file for file in os.listdir(filedir)])
#print(files)
cols = ['open', 'high', 'low', 'close']
df_list = []
for file in tqdm(files):
    print(file)
    df = pd.read_hdf(file)
    df['date'] = [x[:10] for x in list(df['timestamp'])]
    df = df.groupby(['symbol', 'date'])['ltp'].agg(
            open = 'first',
            high = 'max', 
            low = 'min', 
            close = 'last').reset_index()
    print(file)
    df_list.append(df)

df = pd.concat(df_list).reset_index(drop = True)
df['pc'] = df.groupby('symbol')['close'].transform('shift')
df = df[~df['pc'].isna()]
df['tr'] = df.apply(lambda x: round_x(max([x['high'] - x['low'], abs(x['high'] - x['pc']), abs(x['low'] - x['pc'])])), axis = 1)
df['tr_hl'] = df.apply(lambda x: round_x(x['high'] - x['low']), axis = 1)
df['median_atr']  = [round(x, 3) for x in df.groupby(['symbol'])['tr'].transform(lambda x: x.rolling(15).median())]
df['median_atr_hl']  = [round(x, 3) for x in df.groupby(['symbol'])['tr_hl'].transform(lambda x: x.rolling(15).median())]
df['atr']  = [round(x, 3) for x in df.groupby(['symbol'])['tr'].transform(lambda x: x.rolling(14, closed = 'left').mean())]
df['atr_hl']  = [round(x, 3) for x in df.groupby(['symbol'])['tr_hl'].transform(lambda x: x.rolling(14, closed = 'left').mean())]

print(df.tail())
df.to_csv("pc_atr.csv", index = False)
