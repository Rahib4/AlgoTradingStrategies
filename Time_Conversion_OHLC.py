# -*- coding: utf-8 -*-
"""
Created on Sat Sep  5 09:43:07 2020

@author: hashmy
Script to convert 1 min data into 10,15,30 mins data
"""

df_reshape = df[['DateTime','Open','High','Low','Close','Volume']].copy()
df_reshape['DateTime'] = pd.to_datetime(df_reshape['DateTime'])
df_reshape.set_index('DateTime',inplace = True)


type()

ohlc_dict = {                                                                                                             
'Open':'first',                                                                                                    
'High':'max',                                                                                                       
'Low':'min',                                                                                                        
'Close': 'last',                                                                                                    
'Volume': 'sum'
}

df_5min = df_reshape.resample('5min').agg(ohlc_dict)
df_10min = df_reshape.resample('10min').agg(ohlc_dict)
df_15min = df_reshape.resample('15min').agg(ohlc_dict)
df_30min = df_reshape.resample('30min').agg(ohlc_dict)
df_1hr = df_reshape.resample('1H').agg(ohlc_dict)
df_2hr = df_reshape.resample('2H').agg(ohlc_dict)
df_1day = df_reshape.resample('1D').agg(ohlc_dict)

