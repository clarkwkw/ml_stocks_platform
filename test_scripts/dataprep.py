from DataPreparation import TrainingDataPreparation, TestingDataPreparation, ValidationDataPreparation
import pandas as pd

def test():
    stock_data = pd.read_csv('./test_data/10tickers_wo_sector_fill.csv')

    stock_data.drop('sector',axis=1,inplace=True)

    stock_data.dropna(subset=['last_price'],inplace=True)

    stock_data = ValidationDataPreparation(stock_data, True, 10,15,1, period = 3)

    print(stock_data[3][1])

    #print(splited_data[3][1])