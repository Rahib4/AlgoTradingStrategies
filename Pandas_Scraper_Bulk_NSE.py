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
chat_id = -493172276 #725666850 - FuturesBot, -493172276 = InstaTrig Chat ID
todays_date = (datetime.today() - timedelta(days=1)).strftime('%d-%m-%Y')

for i in range(1,16):
  print(i)
  if i < 15:
    link2 = "https://www.nseindia.com/api/block-deal?csv=true"
    urlData = requests.get(link2, headers=header).content
    df_bulk_nse_deal = pd.read_csv(io.StringIO(urlData.decode('utf-8')))
    link2_filename = str("C:\\Saiyad_ADT\\Learning\\Sensex\\InstaTrig\\DownloadedFiles\\") + str('Bulk_Deal_NSE_') + str(todays_date) + str('.csv')
    ## check if there are any new deals 
    if path.exists(link2_filename):
        # Reading the old file present
        df_old_nse = pd.read_csv(link2_filename)
        # Writting the new file after reading the old file
        df_bulk_nse_deal.to_csv(link2_filename)
        # Comparing if the old file contains any of the exsiting deals
        df_bulk_nse_deal = df_bulk_nse_deal[~df_bulk_nse_deal.isin(df_old_nse)].dropna()
    else:
        df_bulk_nse_deal.to_csv(link2_filename)
    print('Bulk Deal - NSE Data download Completed')
    if len(df_bulk_nse_deal) > 0:
        # Sending message
        Message = 'InstaTrig Notification - New Bulk Deal info on NSE!'
        url = str('https://api.telegram.org/bot') + str(bot_token) + str('/sendMessage?chat_id=') + str(chat_id) + str('&text=') + str(Message)
        requests.get(url)
        print(Message)
        # Sending File
        send_doc_command  = str('curl -v -F "chat_id=') + str(chat_id) + str('" -F document=@C:/Saiyad_ADT/Learning/Sensex/InstaTrig/DownloadedFiles/') + str('Bulk_Deal_NSE_') + str(todays_date) + str('.csv https://api.telegram.org/bot') + str(bot_token) + str('/sendDocument') 
        os.system(send_doc_command)

    print('NSE - Bulk Deals - ',i,' - ',(datetime.now()))
    time.sleep(1800)
    
  else:
    link2 = "https://www.nseindia.com/api/block-deal?csv=true"
    urlData = requests.get(link2, headers=header).content
    df_bulk_nse_deal = pd.read_csv(io.StringIO(urlData.decode('utf-8')))
    link2_filename = str("C:\\Saiyad_ADT\\Learning\\Sensex\\InstaTrig\\DownloadedFiles\\") + str('Bulk_Deal_NSE_') + str(todays_date) + str('.csv')
    df_bulk_nse_deal.to_csv(link2_filename)
    print('Bulk Deal - NSE Data download Completed')
    if len(df_bulk_nse_deal) > 0:
        # Sending message
        Message = 'InstaTrig Notification - EOD Bulk Deal report from NSE!'
        url = str('https://api.telegram.org/bot') + str(bot_token) + str('/sendMessage?chat_id=') + str(chat_id) + str('&text=') + str(Message)
        requests.get(url)
        print(Message)
        # Sending File
        send_doc_command  = str('curl -v -F "chat_id=') + str(chat_id) + str('" -F document=@C:/Saiyad_ADT/Learning/Sensex/InstaTrig/DownloadedFiles/') + str('Bulk_Deal_NSE_') + str(todays_date) + str('.csv https://api.telegram.org/bot') + str(bot_token) + str('/sendDocument') 
        os.system(send_doc_command)
    else:
        # Sending message
        Message = 'InstaTrig Notification - No updates on NSE Bulk deals for today!'
        url = str('https://api.telegram.org/bot') + str(bot_token) + str('/sendMessage?chat_id=') + str(chat_id) + str('&text=') + str(Message)
        requests.get(url)
        print(Message)

  
