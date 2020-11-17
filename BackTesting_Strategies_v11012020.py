# -*- coding: utf-8 -*-
"""
Created on Sun Sep  6 00:32:58 2020

@author: hashmy
Code for performing multiple operations on data
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
from scipy.signal import find_peaks
import datetime as dt
import math
import warnings
import datetime
from ta import add_all_ta_features
from ta.utils import dropna
import shutil
import os
import glob
import re

warnings.filterwarnings("ignore")

# For Reading 1 min data, This file has been created by me.. There is another method which aggregates data after downloading the files
def read_file_1min(filepaths_1min,columns_1min):
    data_all = pd.DataFrame()
    for filepath_1min in filepaths_1min:
        print(filepath_1min)
        if len(filepath_1min) < 2:
            filepath_1min = 'C:\\Saiyad ADT\\Learnnig\\Sensex\\Codes\\Derivatives\\1minData\\All_July1_2020_Sep10_2020.txt'
        columns_1min = ["ScriptID", "Date","Time","Open", "High","Low","Close","Volume","Extra"]
        data_Tikr = pd.read_csv(filepath_1min, header=None,  names=columns_1min)
        data_all = data_all.append(data_Tikr)
    return data_all

# For Reading bhavcopy data. This data is for 1 day interval
def read_file_1day(filepath,columns):
    filepath_1day = 'C:\\Saiyad ADT\\Learnnig\\Sensex\\Codes\\R Codes\\Merge\\BSE_All_Data_2014_11_24_to_2020_08_09.txt'    
    columns_1day = ["ScriptID", "DateID", "Open", "High","Low","Close","Volume"]
    data_all = pd.read_csv(filepath_1day, header=None,  names=columns_1day)
    return data_all
    
def filter_data_1min(data_all,Tikrs):
    df_filter = (data_all[data_all['ScriptID'].isin(Tikrs)]).reset_index(drop = True)
    df_filter['Date'] = (pd.to_datetime(df_filter.Date, format = '%Y%m%d'))
    df_filter['Time'] = pd.to_datetime(df_filter.Time, format = '%H:%M').dt.time
    df_filter.sort_values(by=['ScriptID','Date','Time'], inplace=True)
    df_filter['DateTime'] = df_filter['Date'].astype(str) + " " + df_filter['Time'].astype(str)
    df_filter['Intraday'] = df_filter['Open'] - df_filter['Close']
    df_filter['PrevDay'] = df_filter['Open'] - df_filter['Close'].shift(1)    
    return df_filter

# Code to adjust stock split and bonus
#df_filter = df_Tikr.copy()
#tikr = Tikr
#df_split_adj.dtypes
def Adjust_stock_splits(tikr,df_filter):
    filepath_ca = 'C:\\Saiyad ADT\\Learnnig\\Sensex\\Data Download\\Corporate_Actions_BSE_2017_2020.csv'    
    df_split = pd.read_csv(filepath_ca, header=0)
    df_split = df_split.rename(columns = {"Security Name":"Tikr","Ex Date":"Ex_Date"})
    df_split['Category'] = ''
    df_split['Category'][df_split['Purpose'].str.contains('Split')] = 'Split'
    df_split['Category'][df_split['Purpose'].str.contains('Bonus')] = 'Bonus'
    
    df_split['Purpose'] = df_split['Purpose'].replace(" to ",":" ,regex = True)
    df_split['Purpose'] = df_split['Purpose'].str.replace('[a-z]+','')
    df_split['Purpose'] = df_split['Purpose'].str.replace('[A-Z]+','')
    charsToRemove = ["$", ".", "€","/","-"]
    df_split['Purpose'] = df_split['Purpose'].str.replace(r"[{}]+".format("".join([re.escape(x) for x in charsToRemove])),"", regex=True)
    df_split['Purpose'] = df_split['Purpose'].str.strip()
    df_split['Ex_Date'] = pd.to_datetime(df_split['Ex_Date'], format = '%d-%b-%y')
    df_split[['Pre', 'Post']] = df_split['Purpose'].str.split(':', 1, expand=True)
    df_split['Multiplier'] = pd.to_numeric(df_split['Pre'],errors='coerce')/(pd.to_numeric(df_split['Pre'],errors='coerce') + pd.to_numeric(df_split['Post'],errors='coerce'))
    
    df_split = df_split[df_split['Tikr'] == tikr]
    df_split_adj = pd.merge(df_filter, df_split[['Tikr','Ex_Date','Multiplier']], how='left',left_on=['ScriptID'], right_on=['Tikr'])
    # Pending - To map the numbers and divide the numbers before Start Date
    #sample = df_split_adj.head(2000)
    for i in (['Open','High','Low','Close']):
        df_split_adj[i] = np.where(df_split_adj['Date'] < df_split_adj['Ex_Date'],df_split_adj[i]*df_split_adj['Multiplier'],df_split_adj[i])
    return df_split_adj

units = [5,10,15,20,30,40,50,60]
def moving_average(df,units):
    df_ma = df.copy()
    df_ma['DailyPrice'] = df['Open']
    for unit in units:
        print(unit)
        col_header = str('ma_')+str(unit)
        col_header_delta = str('ma_delta')+str(unit)
        df_ma[col_header] = df_ma['DailyPrice'].rolling(unit).mean()
        df_ma[col_header_delta] = (df_ma['DailyPrice'] / df_ma['DailyPrice'].rolling(unit).mean()) - 1
        return df_ma
            
#Timeunit4conversion = '30min'
#df = df_filter
def reshape_ohlc(df,Timeunit4conversion):
    df_reshape = df[['DateTime','Open','High','Low','Close','Volume']].copy()
    df_reshape['DateTime'] = pd.to_datetime(df_reshape['DateTime'])
    df_reshape['Timestamp'] = pd.to_datetime(df_reshape['DateTime'])
    df_reshape.set_index('Timestamp', inplace = True)
    
    ohlc_dict = {                                                                                                             
    'Open':'first',                                                                                                    
    'High':'max',                                                                                                       
    'Low':'min',                                                                                                        
    'Close': 'last',                                                                                                    
    'Volume': 'sum',
    'DateTime':'min'}
    
    ## For Mins use --> 5min, 10min, 15min
    ## For Hours use --> 1H, 2H, 3H
    ## For Days use --> 1H, 2H, 3H
    df_ohlc = df_reshape.resample(Timeunit4conversion).agg(ohlc_dict)
    df_ohlc = (df_ohlc[df_ohlc['High'].notna()]) # Removing all na/null values
    return df_ohlc
    

def crossover(df,series1,series2):
    position = df[series1] > df[series2]
    pre_position = position.shift(1)
    crossover_up = np.where(((position == False) | (pre_position == True)), np.nan, 1)
    crossover_down = np.where(((position == True) | (pre_position == False)), np.nan, 1)
    return crossover_up,crossover_down
    
def PnL_calculation(df,Tikr):
    df_pnl = df[['DateTime','Open','High','Low','Close','Volume','Order','OrderID']]
    df_pnl['Tikr'] = Tikr
    df_pnl = df_pnl[(df_pnl.OrderID != 0)]
    df_buy = pd.pivot_table(df_pnl[df_pnl['Order'] == 'Buy'],index=['Tikr','OrderID','DateTime'],values="Close",columns='Order',fill_value=0) 
    df_buy = df_buy.reset_index().set_index(['Tikr','OrderID'])
    df_sell = pd.pivot_table(df_pnl[df_pnl['Order'] == 'Sell'],index=['Tikr','OrderID','DateTime'],values="Close",columns='Order',fill_value=0) 
    df_sell = df_sell.reset_index().set_index(['Tikr','OrderID'])
    # If we do only Long trades
    df_long = pd.merge(df_buy, df_sell, on=['Tikr','OrderID'])
    df_long = df_long.rename(columns = {"DateTime_x":"DT_Buy","DateTime_y":"DT_Sell"}) #renaming the columns
    df_long['PnL'] = df_long['Sell'] - df_long['Buy']
    
    # If we do only Short trades
    df_sell = df_sell.reset_index()
    df_sell['OrderID'] = df_sell['OrderID'] + 1
    df_sell = df_sell.set_index(['Tikr','OrderID'])
    df_short = pd.merge(df_sell,df_buy,  on=['Tikr','OrderID'])
    df_short = df_short.rename(columns = {"DateTime_x":"DT_Buy","DateTime_y":"DT_Sell"}) #renaming the columns
    df_short['PnL'] = df_short['Sell'] - df_short['Buy'] 
    
    return df_long,df_short

def strategy_diagnostic(df):
    print('Total Profit - ',sum(df['PnL']))
    print('Starting Amount - ',(df['Buy'][:4]).mean())
    print('Rate of return - ', (sum(df['PnL'])/(df['Buy'][:4]).mean())*100)
    print('Percentage Profitable - ',((sum(np.where(df['PnL']>0,1,0))/len(df))*100))
    print('Total # of Txns- ',len(df))
    print('Buy & Hold Return- ',((df['Sell'][-4:].mean())/(df['Buy'][:4].mean())-1)*100)
    ax = df['PnL'].plot.hist(bins=200)
    date_delta = np.round(((df['DT_Sell'][-1:][0] - df['DT_Buy'][:1][0]).days)/365.25)
    CAGR_BnH = (((df['Sell'][-4:].mean())/(df['Buy'][:4].mean()))**(1/date_delta)-1)*100
    CAGR_Trading = (((sum(df['PnL'])+(df['Buy'][:4].mean()))/(df['Buy'][:4].mean()))**(1/date_delta)-1)*100
    print('The Trading activity will result in ', f'{CAGR_Trading:.2f}', '% return')
    print('The Buy and Hold will result in ', f'{CAGR_BnH:.2f}', '% return')
    
    
# Ths code copies data from multiple folders and append them to get the dataframe
def copy_and_append_date(TikrList,years):
    #TikrList = ['TCS']
    output_files = []
    for Tikr in TikrList:
         Tikr = Tikr
         month = ('JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC')
         months = int(6)
         days = int(1)
         CountTikr = 30
    
         ######################################## Get the data ########################################
         FileAddress = str('C:\\Saiyad ADT\\Learnnig\\Sensex\\Algo Trade\\Momentum Analysis\\1minData\\')
         Destination_FileAddress = str("C:\\Saiyad ADT\\Learnnig\\Sensex\\Algo Trade\\Momentum Analysis\\1minData\\Combined\\Monthly\\")
         #year = 2018
         #i = 'JAN'
         for year in years:
             for i in month:
                 try:
                     # month =  "AUG" #str(month).zfill(2)
                     # days = str(days).zfill(2)
                     if year < 2017:
                         source_address = FileAddress + str(year) + str('\\') + str(i) + str('\\') + str(Tikr) + str(".txt")
                     else:
                         source_address = FileAddress + str(year) + str('\\') + str(i) + str('\\') + str("NIFTY50_") + str(i) + str(year) + str('\\') + str(Tikr) + str(".txt")
                     # print(source_address)
    
                     Destination_Filename = Destination_FileAddress + str(Tikr) + str("_") + str(i) + str(year) + str('.txt')
                     # print(Destination_Filename)
    
                     shutil.copy(source_address, Destination_Filename)
                     print(str(Tikr) + str("_") + str(year) + str("_") + str(i) + str(' - Successful'))
                     # days = int(days) + 1
    
                 except:
                     print(str(Tikr) + str("_") + str(year) + str("_") + str(i) + str(' - Failed'))
                     days = int(days) + 1
                     pass
    
         files = glob.glob(r"C:/Saiyad ADT/Learnnig/Sensex/Algo Trade/Momentum Analysis/1minData/Combined/Monthly/*.txt")
         outputfile = str("C:/Saiyad ADT/Learnnig/Sensex/Algo Trade/Momentum Analysis/1minData/Combined/All_data/") + str(
             Tikr) + str("_ALL.txt")
         output_files.append(outputfile)
         with open(outputfile, 'wb') as wfd:
             for f in files:
                 with open(f, 'rb') as fd:
                     shutil.copyfileobj(fd, wfd, 1024 * 1024 * 10)
    
         for f in files:  # Removing temporary files
             os.remove(f)
    return output_files
    
if __name__ == '__main__':
    # there are two options one is latest data.. Jul-Sep2020 - use 'Hardcoded' for this option
    # the other option is complete 3 years of data, you can write anything other than Hardcoded for it
    method = 'Extra'
    columns = ["ScriptID", "Date","Time","Open", "High","Low","Close","Volume","Extra"]
    Tikrs = ['NIFTY'] #,'TCS',RELIANCE, SBIN, TITAN
    years = [2012,2013,2014,2015,2016,2017,2018,2019]
    for Tikr in Tikrs:
        if method == 'Hardcoded':
            filepath = 'C:\\Saiyad ADT\\Learnnig\\Sensex\\Codes\\Derivatives\\1minData\\All_July1_2020_Sep30_2020.txt'
            data_all = read_file_1min(filepath,'x')
        else:
            filepath =  copy_and_append_date(Tikrs,years) # If you are using the 1min data appending 
            data_all = read_file_1min(filepath,'x')
    
        df_filter = filter_data_1min(data_all,Tikrs)
        if Tikr in ['NIFTY','BANKNIFTY']:
            df_split_adj = df_filter.copy()
        else:
            df_split_adj = Adjust_stock_splits(Tikr,df_filter)
        df_ohlc = reshape_ohlc(df_split_adj,'30min') #'3    0min','1H'
    #sample = df_split_adj.head(200)
    ## Run Strategy Now and the output should be in df
    ## The final dataframe should have order & order Id in it 
    df_long_pnl,df_short_pnl = PnL_calculation(df,Tikr) # This data frame contains the transactions data
    strategy_diagnostic(df_long_pnl)
    strategy_diagnostic(df_short_pnl)
    
    
    