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

warnings.filterwarnings("ignore")

def read_file_1min(filepath_1min,columns_1min):
    filepath_1min = 'C:\\Saiyad ADT\\Learnnig\\Sensex\\Codes\\Derivatives\\1minData\\All_July1_2020_Sep10_2020.txt'
    columns_1min = ["ScriptID", "Date","Time","Open", "High","Low","Close","Volume","Extra"]
    data_all = pd.read_csv(filepath_1min, header=None,  names=columns_1min)
    return data_all

def read_file_1day(filepath,columns):
    filepath_1day = 'C:\\Saiyad ADT\\Learnnig\\Sensex\\Codes\\R Codes\\Merge\\BSE_All_Data_2014_11_24_to_2020_08_09.txt'    
    columns_1day = ["ScriptID", "DateID", "Open", "High","Low","Close","Volume"]
    data_all = pd.read_csv(filepath_1day, header=None,  names=columns_1day)
    return data_all
    
def filter_data_1min(data_all,Tikrs):
    df_filter = (data_all[data_all['ScriptID'].isin(Tikrs)].copy()).reset_index(drop = True)
    df_filter['Date'] = (pd.to_datetime(df_filter.Date, format = '%Y%m%d'))
    df_filter['Time'] = pd.to_datetime(df_filter.Time, format = '%H:%M').dt.time
    df_filter['DateTime'] = df_filter['Date'].astype(str) + " " + df_filter['Time'].astype(str)
    df_filter['Intraday'] = df_filter['Open'] - df_filter['Close']
    df_filter['PrevDay'] = df_filter['Open'] - df_filter['Close'].shift(1)    
    return df_filter

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
    df_pnl2 = pd.pivot_table(df_pnl, index=['Tikr','OrderID'],values="Close",columns='Order',fill_value=0) 
    df_pnl2 = df_pnl2.drop(['nan'], axis=1, inplace=False)
    df_pnl2['PnL'] = df_pnl2['Sell'] - df_pnl2['Buy']
    df_pnl2 = df_pnl2[(df_pnl2.Sell != 0)]
    return df_pnl2

def strategy_diagnostic(df):
    print('Total Profit - ',sum(df['PnL']))
    print('Starting Amount - ',(df['Buy'][:4]).mean())
    print('Rate of return - ', (sum(df['PnL'])/(df['Buy'][:4]).mean())*100)
    print('Percentage Profitable - ',((sum(np.where(df['PnL']>0,1,0))/len(df))*100))
    print('Total # of Txns- ',len(df))
    print('Buy & Hold Return- ',((df_pnl['Sell'][-4:].mean())/(df['Buy'][:4].mean())-1)*100)
    
    
if __name__ == '__main__':
    filepath = 'C:\\Saiyad ADT\\Learnnig\\Sensex\\Codes\\Derivatives\\1minData\\All_July1_2020_Sep10_2020.txt'
    columns = ["ScriptID", "Date","Time","Open", "High","Low","Close","Volume","Extra"]
    Tikrs = ['NIFTY'] #,'TCS'
    data_all = read_file_1min('x','x')
    df_filter = filter_data_1min(data_all,Tikrs)
    df_ohlc = reshape_ohlc(df_filter,'1H') #'30min'
    ## Run Strategy Now and the output should be in df
    ## The final dataframe should have order & order Id in it 
    df_pnl = PnL_calculation(df,'NIFTY') # This data frame contains the transactions data
    strategy_diagnostic(df_pnl)
    