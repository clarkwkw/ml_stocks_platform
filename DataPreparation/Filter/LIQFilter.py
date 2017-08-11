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
    if ticker_data.shape[0] <= 1:
        return ticker, float("-inf")
        
    ticker_data["prev_price"] = np.NAN 
    ticker_data.loc[1:, "prev_price"] = ticker_data.loc[0:(ticker_data.shape[0]-1), "last_price"]
    ticker_data["net_return"] = (1.0*ticker_data["last_price"]/ticker_data["prev_price"] - 1)*100
    ticker_data["sign_log_v_p"] = 1
    ticker_data.loc[ticker_data["net_return"] < 0, "sign_log_v_p"] = -1

    # modified the equation log(v(t)p(t)) in the financial paper by log(1+v(t)p(t)) to prevent overflow of small value
    ticker_data["sign_log_v_p"] *= np.log(1+ticker_data["last_price"] * ticker_data["volume"])

    regression_data = ticker_data[["net_return", "sign_log_v_p"]]
    regression_data = regression_data.drop([np.NAN, float("inf"), float("-inf")])

    if regression_data.shape[0] > 0:
        lr = LinearRegression()
        lr.fit(regression_data["sign_log_v_p"], regression_data["net_return"])
        return ticker, lr.coef_[0]
    else:
        return ticker, float("-inf")

def LIQFilter(stock_data):

    ticker_lambda = apply_parallel(stock_data.groupby(['ticker']), calculate_lambda)
        
    #Sort the tickers based on lambda and select top 50%
    ticker_lambda.sort(key=lambda x:x[1], reverse=True)

    filtered_stock = [ticker_lambda[i][0] for i in range(int(len(tickers)/2))]
    
    return stock_data.loc[stock_data['ticker'].isin(filtered_stock)]