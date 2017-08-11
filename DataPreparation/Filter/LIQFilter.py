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

    # Suppress warning
    ticker_data.is_copy = False
    
    ticker_data["prev_price"] = np.NAN 
    ticker_data.loc[1:, "prev_price"] = ticker_data.loc[0:(ticker_data.shape[0]-1), "last_price"]
    ticker_data["net_return"] = (1.0*ticker_data["last_price"]/ticker_data["prev_price"] - 1)*100
    ticker_data["sign_log_v_p"] = 1
    ticker_data.loc[ticker_data["net_return"] < 0, "sign_log_v_p"] = -1

    # modified the equation log(v(t)p(t)) in the financial paper by log(1+v(t)p(t)) to prevent overflow of small value
    ticker_data["sign_log_v_p"] *= np.log(1+ticker_data["last_price"] * ticker_data["volume"])

    ticker_data.replace([np.inf, -np.inf], np.nan, inplace = True)
    ticker_data.dropna(subset = ["sign_log_v_p", "net_return"], how = "any", inplace = True)

    if ticker_data.shape[0] > 0:
        lr = LinearRegression()
        lr.fit(ticker_data["sign_log_v_p"].as_matrix().reshape(-1, 1), ticker_data["net_return"].as_matrix().reshape(-1, 1))
        return ticker, lr.coef_[0]
    else:
        return ticker, float("-inf")

def LIQFilter(stock_data):

    ticker_lambda = apply_parallel(stock_data.groupby(['ticker']), calculate_lambda)
        
    #Sort the tickers based on lambda and select top 50%
    ticker_lambda.sort(key=lambda x:x[1], reverse=True)

    filtered_stock = [ticker_lambda[i][0] for i in range(int(len(ticker_lambda)/2))]
    
    return stock_data.loc[stock_data['ticker'].isin(filtered_stock)]