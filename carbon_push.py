import sys
import time
import os
import platform 
import subprocess
from socket import socket
import pandas as pd
import datetime;
from datetime import timedelta
ts = datetime.datetime.now().timestamp()
print(ts)
CARBON_SERVER = '35.239.157.137'
CARBON_PORT = 2003
delay = 6
symbols=['ACC', 'ADANIENT', 'ADANIPORTS', 'AMARAJABAT', 'AMBUJACEM',
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
symbols=['ADANIENT','RECLTD','BERGEPAINT',
'HINDALCO','EICHERMOT','NAUKRI','COALINDIA','BANDHANBNK','TATACHEM','COLPAL','BIOCON','SUNPHARMA','DIVISLAB','SBILIFE','GLENMARK','CANBK']

symbols=['SUNPHARMA']
#if len(sys.argv) > 1:
#    delay = int( sys.argv[1] )
print(CARBON_SERVER)
sock = socket()
try:
    sock.connect( (CARBON_SERVER,CARBON_PORT) )
except:
    print("Couldn't connect to %(server)s on port %(port)d, is carbon-agent.py running?" % { 'server':CARBON_SERVER, 'port':CARBON_PORT })
    sys.exit(1)
now = int( time.time() )
lines = []
directory=r'/home/exchangeml21_gmail_com/historic_data/30092020'
for filename in os.listdir(directory):
    if((filename.endswith(".NSE.csv")) &( filename[:-8]in symbols)):
        df=pd.read_csv("30092020/"+filename)
        for index,row in df.iterrows():
         ltp=row['LTP']
         date=row['Date']
         ts_time=row['Time']
         date=date.split('/')
         time_list = ts_time.split(':')
         date = list( map(int,date))
         time_list = list(map(int,time_list))
         print(time)
         print(date)
         ts = datetime.datetime(date[2],date[1],date[0],time_list[0],time_list[1],time_list[2]).timestamp()
         ts=ts-330*60
         msg = '%s %s %d\n' % (filename[:-8]+".STOCK", ltp, ts)
         print(msg)
   
         sock.sendall(msg.encode())
         time.sleep(0.0001)
