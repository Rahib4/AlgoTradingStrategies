# -*- coding: utf-8 -*-
"""
Created on Sun Aug 23 12:59:44 2020

@author: hashmy

"""
import urllib.request as urllib2
from urllib.request import Request, urlopen
import requests
import json
from pandas.io.json import json_normalize
import pandas as pd
from datetime import datetime, timedelta, timezone
from pytz import timezone
import os.path
from os import path
import time
from pathlib import Path
import inspect
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from fake_useragent import UserAgent
from bs4 import BeautifulSoup

#https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY
header = {'Accept': '*/*',
           'Accept-Encoding': 'gzip, deflate, br',
           'Accept-Language': 'en-US,en;q=0.9',
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/84.0.4147.135 Safari/537.36',
           'X-Requested-With': 'XMLHttpRequest'}

url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
symbols = ['NIFTY','BANKNIFTY','RELIANCE','BAJAJFINSV','SBIN','TATAMOTORS']  #,]

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("user-data-dir=C:\\Users\\hashmy\\AppData\\Local\\Google\\Chrome\\User Data\\Default")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--window-size=1420,1080')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument("--incognito")
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument(f'user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36')
chrome_options.add_argument("--disable-blink-features")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")

# tikr = 'NIFTY'
# i = 1
# dajs = df = df_json = df_to_write
# 'https://www.nseindia.com/api/option-chain-equities?symbol=BAJFINANCE'

#url = 'https://www.nseindia.com/api/option-chain-equities?symbol=BAJFINANCE'

def URL_creation(tikr):
    if tikr in ['NIFTY','BANKNIFTY']:
        base_url = 'https://www.nseindia.com/api/option-chain-indices?symbol='
    else:
        base_url = 'https://www.nseindia.com/api/option-chain-equities?symbol='
    
    url = base_url + str(tikr)
    print(tikr, '-', url)
    return url

def fetch_json_selenium(tikr,url):
    for x in range(0, 20):  # try 40 times
        driver.delete_all_cookies()
        print(tikr,' - Selenium Attempt: ',x)
        try:
            driver.get("https://www.nseindia.com/option-chain")
            driver.get(url)
            data = driver.page_source
            soup = BeautifulSoup(data, 'html.parser')
            data2 = soup.find('pre').text
            df = json.loads(data2)
            str_error = False
        except Exception:
            str_error = True
            df = None
            pass
        
        if str_error:
            time.sleep(1)  # wait for 2 seconds before trying to fetch the data again
        else:
            break 
        if x == 19:
            print('Trial Limit of 50 attempts reached')
        
    return df

def fetch_json_requests(tikr,url):
    for x in range(0, 20):  # try 40 times
        chrome_session = requests.session()
        chrome_session.cookies.clear()
        print(tikr,' - Requests Attempt: ',x)
        try:
            data = requests.get(url, headers=header).content
            data2 = data.decode('utf-8')
            df = json.loads(data2)
            str_error = False
        except Exception:
            str_error = True
            df = None
            pass
        
        if str_error:
            time.sleep(1)  # wait for 2 seconds before trying to fetch the data again
        else:
            break 
        if x == 49:
            print('Trial Limit of 50 attempts reached')
        
    return df


def fetch_oi_all_expiry( tikr, dajs):
    ce_values = [data['CE'] for data in dajs['records']['data'] if "CE" in data]  # and data['expiryDate'] == expiry_dt]
    pe_values = [data['PE'] for data in dajs['records']['data'] if "PE" in data]  # and data['expiryDate'] == expiry_dt]

    df_ce = pd.DataFrame(ce_values).sort_values(['strikePrice'])
    df_pe = pd.DataFrame(pe_values).sort_values(['strikePrice'])
    df_ce['side'] = 'CE'
    df_pe['side'] = 'PE'
    todays_date = (datetime.today().astimezone(timezone('Asia/Kolkata'))).strftime('%d-%m-%Y %H:%M')
    df_option_chain = df_ce.append(df_pe)
    df_option_chain['Timestamp'] = todays_date
    print('Download complete', ' - ', tikr)
    return df_option_chain


def write_to_csv(df_to_write, tikr):
    location =  str(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))) + str('\\Daily_Data\\OptionChain\\')
    # location = 'C:\\Saiyad ADT\\Learnnig\\Sensex\\Codes\\Option Chain\\Daily_Data\\'
    date_stamp = (datetime.today().astimezone(timezone('Asia/Kolkata'))).strftime('%Y_%m_%d')
    time_stamp = (datetime.today().astimezone(timezone('Asia/Kolkata'))).strftime('%Y_%m_%d_%H_%M')
    output_file = str(location) + str('Options_data_') + str(tikr) + str('_') + str(date_stamp) + str('.csv')
    if path.exists(output_file):
        # Reading the old file present
        df_old_nse = pd.read_csv(output_file, index_col = 0)
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
    current_time = datetime.strptime((datetime.today().astimezone(timezone('Asia/Kolkata'))).strftime('%d-%m-%Y %H:%M'), fmt)
    closing_time = datetime.strptime((datetime.today().astimezone(timezone('Asia/Kolkata'))).strftime('%d-%m-%Y 15:30'), fmt)  ## Market Closing Time is 03:30
    cycle_per_hour = 60 / int(mins)
    no_of_iterations = cycle_per_hour * ((closing_time - current_time).total_seconds() / 3600.0)
    return no_of_iterations


def main():
    for tikr in symbols:
        print(tikr)
        #tikr = 'NIFTY'
        #try:
        url = URL_creation(tikr)
        #df_json = fetch_json_requests(tikr,url) # Uncomment this if you run scraping using reuqests
        df_json = fetch_json_selenium(tikr,url)  # Uncomment this if you run scraping using Selenium
        if len(df_json) > 0:
            df_option_chain = fetch_oi_all_expiry(tikr, df_json)
            write_to_csv(df_option_chain, tikr)
        df_option_chain = pd.DataFrame()
        #except:
        #    print('Error Encountered - ', tikr)


def halt_and_run(lag_time_secs):
    fmt = '%d-%m-%Y %H:%M'
    start_time = datetime.strptime((datetime.today().astimezone(timezone('Asia/Kolkata'))).strftime('%d-%m-%Y 09:14'), fmt)
    closing_time = datetime.strptime((datetime.today().astimezone(timezone('Asia/Kolkata'))).strftime('%d-%m-%Y 15:31'), fmt)
    current_time = datetime.strptime((datetime.today().astimezone(timezone('Asia/Kolkata'))).strftime('%d-%m-%Y %H:%M'), fmt)
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


if __name__ == '__main__':
    driver = webdriver.Chrome(options=chrome_options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.53 Safari/537.36'})
    print(driver.execute_script("return navigator.userAgent;"))
    
    driver.delete_all_cookies()
    driver.maximize_window()
    driver.get("https://www.nseindia.com/option-chain")
    driver.implicitly_wait(5) # seconds

    time_lag_mins = 10
    lag_time_secs = int(time_lag_mins * 60)
    halt_and_run(lag_time_secs)

