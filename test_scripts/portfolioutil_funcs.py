import os
import pandas
import DataPreparation as DataPrep
import PortfolioConstructionUtilities as PortfolioUtils

svm_paras = [{"kernel": "linear"}, {"kernel": "poly"}, {"kernel": "rbf"}, {"kernel": "sigmoid"}]

def test():
	print("Loading data...")
	
	stock_data = pandas.read_csv('./test_data/10tickers_wo_sector_fill.csv')
	stock_data.dropna(subset=['last_price'],inplace=True)
	stock_data.fillna(value = 0, inplace = True)

	os.chdir("./test_output")
	model_dict =  PortfolioUtils.MachineLearningModelDevelopment({"Energy": stock_data}, "SVM", svm_paras, False, 10, 15, 3, 3, "long", period = 5, date = None)
	model = model_dict["Energy"]
	model.save("./model")
	
	PortfolioUtils.StockPerformancePrediction(stock_data, False, "./model/preprocessing/Energy_preprocessing_info.json", "./model", "./portfolio/predicted_value/predicted_value.csv")
	PortfolioUtils.StockRanking("./portfolio/predicted_value/predicted_value.csv", "./portfolio/ranked_stock/ranked_stock.csv")
	PortfolioUtils.StockSelection("./portfolio/ranked_stock/ranked_stock.csv", 5, "./portfolio/sector_portfolio/portfolio.csv", weight_method = "equal")