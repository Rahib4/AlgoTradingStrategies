# -*- coding: utf-8 -*-
"""
Created on Thu Jun 25 12:11:12 2020
InstaTrig code for fetching Bulk Deal & Corporate Announcements from NSE & BSE Sites
@author: hashmy
Telebot - Telegram Bot Codes
Link: https://pypi.org/project/pyTelegramBotAPI/0.3.0/
"""

#tables = pd.read_html("https://apps.sandiego.gov/sdfiredispatch/")
#print(tables[0])

import time
import requests
from datetime import datetime, timedelta
import pandas as pd
import io
import os
import telebot
import os.path
from os import path

header = {
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
  "X-Requested-With": "XMLHttpRequest"
}
TOKEN = '844050261:AAEwvamoXHaB8HYVByjMhNs3V3NIQWy8Vwg'
bot_token = '844050261:AAEwvamoXHaB8HYVByjMhNs3V3NIQWy8Vwg'
tb = telebot.TeleBot(TOKEN)
chat_id = -493172276   #725666850 - FuturesBot, -493172276 = InstaTrig Chat ID
todays_date = (datetime.today()).strftime('%d-%m-%Y')

def BSE_Bulk_Deals():
    link3 = "https://www.bseindia.com/markets/equity/EQReports/block_deals.aspx"
    urlData = requests.get(link3, headers=header).content
    bse_tables = pd.read_html(urlData)
    df_bulk_bse_deal = pd.DataFrame(bse_tables[1])
    df_bulk_bse_deal = df_bulk_bse_deal[pd.to_datetime(df_bulk_bse_deal['Deal Date']) == todays_date]
    return df_bulk_bse_deal
    
for i in range(1,16):
    print(i)
    ## For the first 17 times
    df_bulk_bse_deal = BSE_Bulk_Deals()
    link3_filename = str("C:\\Saiyad_ADT\\Learning\\Sensex\\InstaTrig\\DownloadedFiles\\") + str('Bulk_Deal_BSE_') + str(todays_date) + str('.csv')
    df_bulk_bse_deal.to_csv(link3_filename)
    print('Bulk Deal - BSE Data download Completed')
    if i < 15:
        if len(df_bulk_bse_deal) > 0:
        # Sending message
            Message = 'InstaTrig Notification - New Bulk Deal info on BSE!'
            url = str('https://api.telegram.org/bot') + str(bot_token) + str('/sendMessage?chat_id=') + str(chat_id) + str('&text=') + str(Message)
            requests.get(url)
            
            # Sending File
            send_doc_command  = str('curl -v -F "chat_id=') + str(chat_id) + str('" -F document=@C:/Saiyad_ADT/Learning/Sensex/InstaTrig/DownloadedFiles/') + str('Bulk_Deal_BSE_') + str(todays_date) + str('.csv https://api.telegram.org/bot') + str(bot_token) + str('/sendDocument') 
            os.system(send_doc_command)
        print('BSE - Bulk Deals - ',i,' - ',(datetime.now()))
        # Stopping the code for 30 min
        time.sleep(1800)
    else:
        if len(df_bulk_bse_deal) > 0:
        # Sending message
            Message = 'InstaTrig Notification - EOD Bulk Deal report from BSE!'
            url = str('https://api.telegram.org/bot') + str(bot_token) + str('/sendMessage?chat_id=') + str(chat_id) + str('&text=') + str(Message)
            requests.get(url)
            
            # Sending File
            send_doc_command  = str('curl -v -F "chat_id=') + str(chat_id) + str('" -F document=@C:/Saiyad_ADT/Learning/Sensex/InstaTrig/DownloadedFiles/') + str('Bulk_Deal_BSE_') + str(todays_date) + str('.csv https://api.telegram.org/bot') + str(bot_token) + str('/sendDocument') 
            os.system(send_doc_command)
        else:
            # Sending message
            Message = 'InstaTrig Notification - No updates on BSE Bulk deals for today!'
            url = str('https://api.telegram.org/bot') + str(bot_token) + str('/sendMessage?chat_id=') + str(chat_id) + str('&text=') + str(Message)
            requests.get(url)  
