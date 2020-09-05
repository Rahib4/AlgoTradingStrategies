# -*- coding: utf-8 -*-
"""
Created on Sat Aug 29 05:15:40 2020

@author: hashmy
"""

import urllib.request as urllib2
from urllib.request import Request, urlopen
import requests
import json
from pandas.io.json import json_normalize
import pandas as pd
from datetime import datetime, timedelta
import os.path
from os import path
import time
import io
import os


#https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY

header = {'Accept': '*/*',
           'Accept-Encoding': 'gzip, deflate, sdch, br',
           'Accept-Language': 'en-GB,en-US;q=0.8,en;q=0.6',
           'Connection': 'keep-alive',
           'Host': 'www.nseindia.com',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
           'X-Requested-With': 'XMLHttpRequest'}

print('Script Running')

def fetch_json_method():
    url = 'https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%2050'
    url = url.strip()
    data = requests.get(url, headers=header).content
    data2 = data.decode('utf-8')
    data3 = json.loads(data2)
    df_nifty50 = pd.json_normalize(data3['data'])
    df_nifty50['TimeStamp'] = datetime.strptime(datetime.today().strftime('%d-%m-%Y %H:%M'), '%d-%m-%Y %H:%M')
    return df_nifty50

def write_to_csv(df_to_write):
    location = 'C:\\Saiyad ADT\\Learnnig\\Sensex\\Codes\\Option Chain\\Nifty50\\'
    time_stamp = (datetime.today()).strftime('%Y_%m_%d')
    output_file = str(location) + str('Nifty50_') + str(time_stamp) + str('.csv')
    if path.exists(output_file):
        # Reading the old file present
        df_old_nse = pd.read_csv(output_file)
        # Appending the old file and the new data
        df_to_write = df_old_nse.append(df_to_write, ignore_index=True)
        # Writting the file with latest data for the day
        df_to_write.to_csv(output_file)
        print('File updated with latest data', ' - ', time_stamp)
    else:
        # Since the file is not present in the folder, we can simply write
        df_to_write.to_csv(output_file)
      
def cycles_to_run(mins):
    ## Market opening time is 9:15, and this code should get triggered on time..
    # but if for some reason code is not triggered on time then we want the code to run only till 15:30 PM
    fmt = '%d-%m-%Y %H:%M'
    current_time = datetime.strptime(datetime.today().strftime('%d-%m-%Y %H:%M'), fmt)
    closing_time = datetime.strptime(datetime.today().strftime('%d-%m-%Y 15:30'), fmt)  ## Market Closing Time is 03:30
    cycle_per_hour = 60 / int(mins)
    no_of_iterations = cycle_per_hour * ((closing_time - current_time).total_seconds() / 3600.0)
    return no_of_iterations

def main():
    try:
        df_nifty50= fetch_json_method()
        write_to_csv(df_nifty50)
    except:
        print('Error Encountered - Nifty 50 Download')

def halt_and_run(lag_time_secs):
    fmt = '%d-%m-%Y %H:%M'
    start_time = datetime.strptime(datetime.today().strftime('%d-%m-%Y 09:14'), fmt)
    closing_time = datetime.strptime(datetime.today().strftime('%d-%m-%Y 15:31'), fmt)
    current_time = datetime.strptime(datetime.today().strftime('%d-%m-%Y %H:%M'), fmt)
    iteration = int(cycles_to_run(10))
    print('Total # of iterations calculated - ', iteration)
    for i in range(1, iteration + 1):
        print(i)
        if (current_time - start_time).total_seconds() < 0:
            # Market is closed as of now
            print('Markets havent opened yet, Go back to sleep')
            run_signal = False
            time.sleep(lag_time_secs)  #15 mins lag
        elif (closing_time - current_time).total_seconds() < 0:
            print('Markets have closed')
            run_signal = False
            time.sleep(lag_time_secs)  #15 mins lag
        elif (closing_time - current_time).total_seconds() > 0 and (current_time - start_time).total_seconds() > 0:
            run_signal = True
            iteration = int(cycles_to_run(10))
            main()
            time.sleep(int(lag_time_secs))  #15 mins lag
            print(i)
    

### Method 2 - Simply downloading csv file - Not very reliable
#url = 'https://www.nseindia.com/api/equity-stockIndices?csv=true&index=NIFTY%2050'
#data = requests.get(url, headers=header).content
#data2 = data.decode('utf-8')
#df_nifty50_method2 = pd.read_csv(io.StringIO(data2))

if __name__ == '__main__':
    time_lag_mins = 10
    lag_time_secs = int(time_lag_mins * 60)
    halt_and_run(lag_time_secs)
