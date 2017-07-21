import pandas as pd
import math as m

from functools import partial
from multiprocessing import Pool as ThreadPool 

def sort_return_and_calculate_target(B_top,B_bottom,d):
    data = d[1]

    data.sort_values(by=['volatility-adjusted return'],inplace=True)

    l_top = int(m.ceil(data.shape[0]*(B_top/100.)))
    l_buttom = int(m.ceil(data.shape[0]*(B_bottom/100.)))
    if data.shape[0] == 1:
        l_buttom = 0
    d_top = data.head(l_top).copy()
    d_buttom = data.tail(l_buttom).copy()
    d_top['label'] = 1.0
    d_buttom['label'] = 0.0

    data = pd.concat([d_top, d_buttom])

    return d[0],data

def TargetLabelGeneration(stock_data, B_top, B_bottom, target_label_holding_period):
    if not isinstance(target_label_holding_period, int):
        raise Exception('target_label_holding_period must be a integer.')

    #Split the stock data by ticker
    tickers=stock_data['ticker'].unique()

    ticker_data = {t : pd.DataFrame for t in tickers}

    for key in ticker_data.keys():
        ticker_data[key] = stock_data[:][stock_data.ticker == key] 
        ticker_data[key].sort_values(by=['date'],inplace=True)

    #Calculate the r(t) and vol(t)
    for ticker in tickers:
        t = ticker_data[ticker]
        t_len = t.shape[0]
        r_t = [float('nan') for _ in range(target_label_holding_period)]
        vol_t = [float('nan') for _ in range(target_label_holding_period)]

        kappa = 2./(180+1)
        for i in range(target_label_holding_period,t_len):
            r = (t.iloc[i].last_price - t.iloc[i-target_label_holding_period].last_price) / t.iloc[i-target_label_holding_period].last_price * 100
            if i == target_label_holding_period:
                v = m.fabs(r)
            else:
                v = kappa*m.fabs(r) + (1-kappa)*vol_t[i-1]
            r_t.append(r)
            vol_t.append(v)

        t['return'] = r_t
        t['vol(t)'] = vol_t
        t['volatility-adjusted return'] = t['return']/t['vol(t)']
        t['volatility-adjusted return'] = t['volatility-adjusted return'].shift(-1)
        t['return'] = t['return'].shift(-1)
        t.drop(t.index[len(t)-1], inplace=True)

    #Join the ticker data and split the stock data by date, than sort the tickers based on return
    stock_data = pd.concat([ticker_data[i] for i in ticker_data])

    dates=stock_data['date'].unique()

    date_data = {d : pd.DataFrame for d in dates}

    for key in date_data.keys():
        date_data[key] = stock_data[:][stock_data.date == key] 

    d = [(key,date_data[key]) for key in date_data.keys()]

    pool = ThreadPool() 
    func = partial(sort_return_and_calculate_target, B_top, B_bottom)

    pool_outputs = pool.map(func, d)
    pool.close()
    pool.join()

    for i in pool_outputs:
        date_data[i[0]] = i[1]

    #Join the date data
    stock_data = pd.concat([date_data[i] for i in date_data])
    stock_data.drop(['vol(t)','volatility-adjusted return'],axis=1,inplace=True)

    stock_data.sort_values(by=['date','return'],ascending=[True,False],inplace=True)

    return stock_data

