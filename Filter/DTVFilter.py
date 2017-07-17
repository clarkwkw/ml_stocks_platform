import pandas as pd
import numpy as np
import math as m
from tqdm import tqdm

from multiprocessing import Pool as ThreadPool

def sort_date_data(d):
    data = d[1]

    data.sort_values(by=['e_t'],inplace=True)
    l = data.shape[0]
    data = data.head(int(m.ceil(l/2.)))

    return d[0],data

def DTVFilter(stock_data):
    #Split the stock data by ticker
    tickers=stock_data['ticker'].unique()

    ticker_data = {t : pd.DataFrame for t in tickers}

    for key in ticker_data.keys():
        ticker_data[key] = stock_data[:][stock_data.ticker == key] 
        ticker_data[key].sort_values(by=['date'],inplace=True)

    #Calculate the e(t) of each stock
    for ticker in tickers:
        t = ticker_data[ticker]
        t_len = t.shape[0]
        e_t = [t.iloc[0].last_price*t.iloc[0].volume]
        a = 2./(91+1)

        for i in range(1,t_len):
            e_t.append(a*t.iloc[i].last_price*t.iloc[i].volume+(1-a)*e_t[i-1])
        t['e_t'] = e_t    

    #Join the ticker data and split the stock data by date, than sort the tickers based on e(t)
    stock_data = pd.concat([ticker_data[i] for i in ticker_data])

    dates=stock_data['date'].unique()

    date_data = {d : pd.DataFrame for d in dates}

    for key in date_data.keys():
        date_data[key] = stock_data[:][stock_data.date == key] 

    d = [(key,date_data[key]) for key in date_data.keys()]

    pool = ThreadPool() 

    pool_outputs = pool.map(sort_date_data, d)
    pool.close()
    pool.join()

    for i in pool_outputs:
        date_data[i[0]] = i[1]

    #Join the date data
    stock_data = pd.concat([date_data[i] for i in date_data])

    stock_data.drop('e_t',axis=1, inplace=True)
    stock_data.sort_values(by=['date'],inplace=True)

    return stock_data