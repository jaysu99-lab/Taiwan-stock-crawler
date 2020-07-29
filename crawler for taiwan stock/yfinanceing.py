
#import requests
import pandas as pd
import yfinance as yf
import pymysql

from stockid import stockid
import numpy as np
import time



stock_public = stockid.Stock_public
stock_counter = stockid.Stock_counter

#start_time = "2016-10-01"
List_TW=[]
List_TWO=[]
#上市
for iid in stock_public.values():
    iid=iid.replace(' ','')
    List_TW.append(iid+'.TW')
#上櫃
for iid in stock_counter.values():
    iid = iid.replace(' ', '')
    List_TWO.append(iid+'.TWO')

List_all = List_TW + List_TWO

db = pymysql.connect(host='your host',port=3306,user='your user name',passwd='password',db='Project_test')
cur = db.cursor()

for L in List_all:
    msft = yf.Ticker("{}".format(L))
    hist = msft.history(period="max")# 抓的天數
    a = ['Dividends','Stock Splits']
    df1 = hist.drop(a,axis = 1)
    df1 = df1.reset_index()
    df1 = df1.fillna(0)

    b = 0
    for cp in range(0, len(df1)):
        # 股票代號
        a0 = str(df1.iloc[[cp], [0]])[-10:]
        a1 = np.float(df1.iloc[[cp], [1]].values[0][0])
        a2 = np.float(df1.iloc[[cp], [2]].values[0][0])
        a3 = np.float(df1.iloc[[cp], [3]].values[0][0])
        a4 = np.float(df1.iloc[[cp], [4]].values[0][0])
        a5 = np.int(df1.iloc[[cp], [5]].values[0][0])
        LL = L[0:4]
        sql = 'INSERT INTO daily_trade VALUES({}, \'{}\', {}, {}, {},{},{});'.format(LL, a0, a1, a2, a3, a4, a5)
        b = b + 1
        print(b, sql)
        cur.execute(sql)
        db.commit()
cur.close()
db.close()
# for L in List_TWO:
#     msft = yf.Ticker("{}".format(L))
#     # print(msft.info)
#     hist = msft.history(start=start_time)
#     a = ['Dividends', 'Stock Splits']
#     df = hist.drop(a, axis=1)
#     print('{} 完成'.format(L))
#
# connection = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='H124946510', db='project_test')
#
# cursor = connection.cursor()

