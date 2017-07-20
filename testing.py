from DataPreparation import TrainingDataPreparation, TestingDataPreparation, ValidationDataPreparation, DataPreprocessing
import pandas as pd

if __name__ == '__main__':
    stock_data = pd.read_csv('./test_data/10tickers_wo_sector_fill.csv')

    stock_data.drop('sector',axis=1,inplace=True)

    stock_data.dropna(subset=['last_price'],inplace=True)

    stock_data = ValidationDataPreparation(stock_data, stock_filter_flag = False, B_top = 10, B_buttom = 15, target_label_holding_period = 1, period = 3)

    print("# Folds: %d"%len(stock_data))

    normalized_train, normalized_valid = DataPreprocessing(flag = "validate", stock_data = stock_data[6][0], validate_data = stock_data[6][1])
