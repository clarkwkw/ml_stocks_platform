import pandas as pd
import numpy as np
import math as m
from tqdm import tqdm
from sklearn.linear_model import LinearRegression

from multiprocessing import Pool as ThreadPool

def calculate_r_log_vt_pt(t):
    data = t[1]

    data.sort_values(by=['date'],inplace=True)

    t_len = data.shape[0]
    r_t = [float('nan')]
    log_v_p = [float('nan')]

    for i in range(1,t_len):
        r = (data.iloc[i].last_price - data.iloc[i-1].last_price) / data.iloc[i-1].last_price * 100
        if r >= 0:
            sign = 1
        else:
            sign = -1
        r_t.append(r)
        log_v_p.append(sign*np.log(data.iloc[i].last_price*data.iloc[i].volume))
    data['net_return'] = r_t
    data['sign_log_v_p'] = log_v_p

    return t[0],data

def LIQFilter(stock_data):

    #Split the stock data by ticker
    tickers=stock_data['ticker'].unique()

    ticker_data = {t : pd.DataFrame for t in tickers}

    for key in ticker_data.keys():
        ticker_data[key] = stock_data[:][stock_data.ticker == key] 

    #Calculate the r(t) and log v(t)p(t) of each stock
    t = [(key,ticker_data[key]) for key in ticker_data.keys()]

    pool = ThreadPool() 

    pool_outputs = pool.map(calculate_r_log_vt_pt, t)
    pool.close()
    pool.join()
    
    for i in pool_outputs:
        ticker_data[i[0]] = i[1]      
   
    #Regress lambda of each stock using linear regression
    ticker_lambda = []

    for ticker in tickers:
        t = ticker_data[ticker]
        y_train = t.dropna()['net_return'].as_matrix()
        x_train = t.dropna()['sign_log_v_p'].as_matrix().reshape(-1, 1)

        #Build the regressor
        lr = LinearRegression()
        lr.fit(x_train,y_train)
        ticker_lambda.append([ticker,lr.coef_[0]])
        
    #Sort the tickers based on lambda and select top 50%
    ticker_lambda.sort(key=lambda x:x[1], reverse=True)

    filtered_stock = [ticker_lambda[i][0] for i in range(int(len(tickers)/2))]
    
    return stock_data.loc[stock_data['ticker'].isin(filtered_stock)]