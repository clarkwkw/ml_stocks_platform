import os
import pandas
import DataPreparation as DataPrep
import PortfolioConstructionUtilities as PortfolioUtils

svm_paras = [{"kernel": "linear"}, {"kernel": "poly"}, {"kernel": "rbf"}, {"kernel": "sigmoid"}]

def test():
	print("Loading data...")
	
	energy_data = pandas.read_csv('./test_data/Energy-10.csv')
	energy_data.dropna(subset=['last_price'],inplace=True)
	energy_data.fillna(value = 0, inplace = True)

	materials_data = pandas.read_csv('./test_data/Materials-10.csv')
	materials_data.dropna(subset=['last_price'],inplace=True)
	materials_data.fillna(value = 0, inplace = True)

	stock_data = {"Energy": energy_data, "Materials": materials_data}
	tickers = []
	for sector in stock_data:
		tickers.append(stock_data[sector].loc[:, 'ticker'].unique())

	os.chdir("./test_output")
	
	selling_price = pandas.DataFrame({'ticker': tickers, 'price': 100})

	print("Developing models...")
	models_map = PortfolioUtils.MachineLearningModelDevelopment(stock_data, "SVM", svm_paras, False, 10, 15, 3, 3, "long", period = 5)
	for sector in models_map:
		model = models_map[sector]
		model.save("/model/%s"%sector)

	print("Building portfolio...")
	full_portfolio = PortfolioUtils.PortfolioConstruction(stock_data, 4, False, "test", inter_sector_weight = "equal")
	
	print("Generating full report...")
	PortfolioUtils.PortfolioReportGeneration(full_portfolio, selling_price, 'test')