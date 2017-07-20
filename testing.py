from DataPreparation import TrainingDataPreparation, TestingDataPreparation, ValidationDataPreparation, DataPreprocessing
import pandas as pd

if __name__ == '__main__':
    stock_data = pd.read_csv('./test_data/10tickers_wo_sector_fill.csv')

    stock_data.drop('sector',axis=1,inplace=True)

    stock_data.dropna(subset=['last_price'],inplace=True)

    stock_data = ValidationDataPreparation(stock_data, True, 10,15,1, period = 3)

    normalized_train, normalized_valid = DataPreprocessing(flag = "validate", stock_data = stock_data[0][0], validate_data = stock_data[0][1])
