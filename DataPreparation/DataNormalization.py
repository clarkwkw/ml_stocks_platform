import pandas as pd
import json

from .normalization_method import normalization_method
from .factors_normalization_method import fnm

def analyze_factors_statistics(stock_data):
    try:
        factors = list(stock_data.drop(['record_id','ticker', 'sector', 'date','return','label'],axis=1).columns)
    except:
        factors = list(stock_data.drop(['record_id','ticker', 'sector', 'date'],axis=1).columns)

    #Split the stock data by ticker
    tickers=stock_data['ticker'].unique()

    ticker_data = {t : pd.DataFrame for t in tickers}

    for key in ticker_data.keys():
        ticker_data[key] = stock_data[:][stock_data.ticker == key] 
        ticker_data[key].sort_values(by=['date'],inplace=True)   

    #Calculate the statistics
    normalization_info = {}
    for ticker in ticker_data:
        t = ticker_data[ticker]
        normalization_info[ticker] = {}
        for factor in factors:
            normalization_info[ticker][factor] = {}
            normalization_info[ticker][factor]['max'] = float(t[factor].max())
            normalization_info[ticker][factor]['min'] = float(t[factor].min())
            normalization_info[ticker][factor]['mean'] = float(t[factor].mean())
            normalization_info[ticker][factor]['std'] = float(t[factor].std())

    return normalization_info

def DataNormalization(stock_data, validate_data = None, normalization_info = None):
    try:
        factors = list(stock_data.drop(['record_id','ticker', 'sector', 'date','return','label'],axis=1).columns)
    except:
        factors = list(stock_data.drop(['record_id','ticker','sector','date'],axis=1).columns)

    if normalization_info is None:
        info = analyze_factors_statistics(stock_data)
    else:
        info = normalization_info

    #Read factors normalization method
    factors_method = fnm

    #Split the stock data by ticker
    tickers=stock_data['ticker'].unique()

    ticker_data = {t : pd.DataFrame for t in tickers}

    for key in ticker_data.keys():
        ticker_data[key] = stock_data[:][stock_data.ticker == key] 
        ticker_data[key].sort_values(by=['date'],inplace=True)   

    #Normalize the stock data
    unseen_stock = []
    for ticker in ticker_data:
        t = ticker_data[ticker]
        try:
            i = info[ticker]
        except KeyError:
            unseen_stock.append(ticker)
            continue
        for factor in factors:
            t[factor] = normalization_method[factors_method[factor]['method']](t[factor].as_matrix(),i[factor])
            
    for s in unseen_stock:
        ticker_data.pop(s,None)

    #Join the ticker data
    stock_data = pd.concat([ticker_data[i] for i in ticker_data])   
    stock_data.sort_values(by=['date'],inplace=True)

    if validate_data is not None:
        #Split the validate data by ticker
        tickers=validate_data['ticker'].unique()

        ticker_data = {t : pd.DataFrame for t in tickers}

        for key in ticker_data.keys():
            ticker_data[key] = validate_data[:][validate_data.ticker == key] 
            ticker_data[key].sort_values(by=['date'],inplace=True)   

        import warnings
        #Normalize the validate data
        for ticker in ticker_data:
            t = ticker_data[ticker]
            t[factor] = normalization_method[factors_method[factor]['method']](t[factor].as_matrix(),info[ticker][factor])
                
        #Join the ticker data
        validate_data = pd.concat([ticker_data[i] for i in ticker_data])   
        validate_data.sort_values(by=['date'],inplace=True)

    #Return the desired output
    if validate_data is not None:
        return (stock_data, validate_data)

    if normalization_info is None:
        return (stock_data, info)

    return stock_data