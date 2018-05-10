import pandas as pd
import math as m
import numpy as np
import config
from functools import partial
from multiprocessing import Pool as ThreadPool 

def apply_parallel(dfGrouped, func, **kwargs):
    result = []
    pool = ThreadPool() 
    func = partial(func, **kwargs)
    result = pool.map(func, [group for _, group in dfGrouped])
    pool.close()
    pool.join()
    return result

def vol_calculator_factory(target_label_holding_period, kappa):
    prev_last_price = []
    prev_vol = []
    def vol_calculator(row):
        cur_price, vol = row["last_price"], np.NAN
        if len(prev_last_price) >= target_label_holding_period:
            old_price = prev_last_price[len(prev_last_price) - target_label_holding_period]
            if old_price != 0:
                row["return"] = (1.0*cur_price/old_price - 1)*100
            else:
                row["return"] = np.NAN
            if len(prev_last_price) == target_label_holding_period:
                vol = m.fabs(row["return"])
            else:
                vol = kappa*m.fabs(row["return"]) + (1-kappa)*prev_vol[len(prev_last_price) - 1]
            row["vol(t)"] = vol
            if row["vol(t)"] != 0:
                row["volatility-adjusted return"] = row["return"]/row["vol(t)"]
            else:
                row["volatility-adjusted return"] = np.NAN

        prev_last_price.append(cur_price)
        prev_vol.append(vol)

        return row
    return vol_calculator

def calculate_vol(ticker_data, target_label_holding_period, kappa = 2./(180+1)):
    ticker_data = ticker_data.apply(vol_calculator_factory(target_label_holding_period, kappa), axis = 1).copy()
    ticker_data['volatility-adjusted return'] = ticker_data['volatility-adjusted return'].shift(-1)
    ticker_data['return'] = ticker_data['return'].shift(-1)
    ticker_data.drop(ticker_data.index[ticker_data.shape[0]-1], inplace=True)

    # Some stocks may have NA values on return/volatility-adjusted return
    ticker_data.dropna(axis = 0, subset = ['return', 'volatility-adjusted return'], inplace = True)
    return ticker_data

def sort_and_label(stock_data, B_top, B_bottom):
    stock_data = stock_data.sort_values(by = ['volatility-adjusted return'], inplace = False, ascending = False).copy()
    l_top = int(m.ceil(stock_data.shape[0]*(B_top/100.)))
    l_bottom = int(m.ceil(stock_data.shape[0]*(B_bottom/100.)))
    if l_top + l_bottom < stock_data.shape[0]:
        selected_head, selected_tail = None, None
        if l_top > 0:
            selected_head = stock_data.head(l_top).copy()
        if l_bottom > 0:
            selected_tail = stock_data.tail(l_bottom).copy()
            selected_tail["label"] = 0
        stock_data = pd.concat([selected_head, selected_tail])
    return stock_data

def TargetLabelGeneration(stock_data, B_top, B_bottom, target_label_holding_period):
    if not isinstance(target_label_holding_period, int):
        raise Exception('target_label_holding_period must be a integer.')

    stock_data["return"] = np.NAN
    stock_data["vol(t)"] = np.NAN
    stock_data["volatility-adjusted return"] = np.NAN
    stock_data["label"] = 1

    # Split the stock data by ticker and calculate r(t) and vol(t)
    stock_datas = apply_parallel(stock_data.groupby(['ticker'], group_keys = False), calculate_vol, target_label_holding_period = target_label_holding_period)

    stock_data = pd.concat(stock_datas)
    grouped = stock_data.groupby(['date'], group_keys = False)

    # Split the stock data by date and sort the tickers based on return
    stock_datas = apply_parallel(grouped, sort_and_label, B_top = B_top, B_bottom = B_bottom)

    stock_data = pd.concat(stock_datas)
    
    stock_data.drop(['vol(t)', 'volatility-adjusted return'], axis = 1, inplace = True)

    stock_data.sort_values(by = ['date','return'], ascending = [True, False],inplace = True)

    return stock_data

