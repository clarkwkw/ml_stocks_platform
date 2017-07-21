import pandas as pd

def ValidationSetSplit(stock_data, percentage = None, period = None, date = None):
    if period and date:
        raise Exception('period and date can only be used either one')    
    if not period and not date:
        raise Exception('period or date have not defined.')
        
    #Sort the stock data by date first and find the years of the stock data
    stock_data['date'] = pd.to_datetime(stock_data['date'])
    stock_data.sort_values(by=['date'],inplace=True)
    year = stock_data['date'].dt.year.unique()

    #Train-Validate pairs of index of years in each period or in date
    if period is not None:
        index = []
        no_partition = int(len(year)/period)

        if len(year) % period == 0:
            no_partition = no_partition - 1

        for i in range(1, no_partition+1):
            index.append((year[:i*period],year[i*period:(i+1)*period]))
    elif date is not None:
        index = []
        date = int(date)
        index.append((year[year<date],year[year>=date]))    

    #Split the stock data into train set and validate set based on index
    splited_data = []
    for i in index:
        train, validate = stock_data[stock_data['date'].dt.year.isin(i[0])], stock_data[stock_data['date'].dt.year.isin(i[1])]
        splited_data.append((train,validate))
        
    #Temporary solution for eliminate new stock entry in validation set
    elimited_data = []
    for t, v in splited_data:
        t_ticker = t['ticker'].unique()
        v = v[:][v['ticker'].isin(t_ticker)]
        elimited_data.append((t,v))
    
    splited_data = elimited_data
    
    return splited_data