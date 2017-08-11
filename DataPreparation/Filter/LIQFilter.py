import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import config
from concurrent.futures import ThreadPoolExecutor, as_completed

def apply_parallel(dfGrouped, func):
    result = []
    with ThreadPoolExecutor(max_workers = config.max_thread) as executor:
        futures = []
        for _, group in dfGrouped:
            futures.append(executor.submit(func, group))
        for future in as_completed(futures):
            result.append(future.result())
    return result

def calculate_lambda(ticker_data):
    ticker = ticker_data.iloc[0]['ticker']
    if ticker_data.shape[0] <= 1:
        return ticker, float("-inf")

    prev_price = None
    sign_log_v_p = []
    net_returns = []
    for row in ticker_data.itertuples():
        last_price = row.last_price

        if prev_price is not None and last_price != 0:

            net_return = (1.0*last_price/prev_price - 1)*100

            sign = 1
            if net_return < 0:
                sign = -1

            volume = row.volume
            if pd.isnull(volume):
                prev_price = last_price
                continue

            sign_log_v_p.append(np.log(1+last_price * volume))
            net_returns.append(net_return)
        prev_price = last_price
        
    if len(sign_log_v_p) > 0:
        lr = LinearRegression()
        lr.fit(np.asarray(sign_log_v_p).reshape(-1, 1), np.asarray(net_returns).reshape(-1, 1))
        return ticker, lr.coef_[0]
    else:
        return ticker, float("-inf")

def LIQFilter(stock_data):

    ticker_lambda = apply_parallel(stock_data.groupby(['ticker']), calculate_lambda)
        
    #Sort the tickers based on lambda and select top 50%
    ticker_lambda.sort(key=lambda x:x[1], reverse=True)

    filtered_stock = [ticker_lambda[i][0] for i in range(int(len(ticker_lambda)/2))]
    
    return stock_data.loc[stock_data['ticker'].isin(filtered_stock)]