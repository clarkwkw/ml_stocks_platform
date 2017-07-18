import pandas as pd
import numpy as np
import math as m
from tqdm import tqdm

from multiprocessing import Pool as ThreadPool

def sort_last_price(d):
    data = d[1]
    
    data.sort_values(by=['last_price'],inplace=True)
    l = data.shape[0]
    data = data.head(int(m.ceil(l/2.)))

    return d[0],data

def PriceFilter(stock_data):
    #Split the stock data by date then sort by last price
    dates=stock_data['date'].unique()

    date_data = {d : pd.DataFrame for d in dates}

    for key in date_data.keys():
        date_data[key] = stock_data[:][stock_data.date == key] 

    d = [(key,date_data[key]) for key in date_data.keys()]

    pool = ThreadPool() 

    pool_outputs = pool.map(sort_last_price, d)
    pool.close()
    pool.join()

    for i in pool_outputs:
        date_data[i[0]] = i[1]

    #Join the date data
    stock_data = pd.concat([date_data[i] for i in date_data])

    stock_data.sort_values(by=['date'],inplace=True)

    return stock_data