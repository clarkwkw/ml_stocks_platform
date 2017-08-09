from StockOperation import *
import config
from datetime import timedelta
import utils
import numpy as np
import pandas
from ReportFormatting import reformat_report

# Construct portfolio using trained models
# buying_price: a dataframe containing Ticker and Buying Price columns
def PortfolioConstruction(ML_sector_factors, buying_price, trading_stock_quantity, stock_filter_flag, date_prefix, trained_models_map = None, inter_sector_weight = "equal"):
	full_portfolio = None
	n_sectors = len(ML_sector_factors)
	full_portfolio_path = "./portfolio/full_portfolio/%s_full_portfolio.csv"%date_prefix
	for sector in ML_sector_factors:
		stock_data = ML_sector_factors[sector]
		preprocessing_file_path = "./model/preprocessing/%s_preprocessing_info.json"%sector
		model_path = "./model/%s"%sector
		predicted_value_path = "./portfolio/predicted_value/%s_%s_predicted_value.csv"%(date_prefix, sector)
		ranked_stocks_path = "./portfolio/ranked_stock/%s_%s_ranked_stock.csv"%(date_prefix, sector)
		sector_portfolio_path = "./portfolio/sector_portfolio/%s_%s_portfolio.csv"%(date_prefix, sector)
		trained_model = None
		if trained_models_map is not None:
			trained_model = trained_models_map[sector]
		StockPerformancePrediction(stock_data, buying_price, stock_filter_flag, preprocessing_file_path, model_path, predicted_value_path, trained_model)
		StockRanking(predicted_value_path, ranked_stocks_path)
		sector_portfolio = StockSelection(ranked_stocks_path, trading_stock_quantity, sector_portfolio_path)
		sector_portfolio["sector"] = sector
		if inter_sector_weight == "equal":
			sector_portfolio["weight"] = sector_portfolio["weight"]/n_sectors
		elif type(inter_sector_weight) == dict:
			sector_portfolio["weight"] = sector_portfolio["weight"] * inter_sector_weight[sector]
		else:
			raise Exception("Unexpected type of inter_sector_weight")

		if full_portfolio is None:
			full_portfolio = sector_portfolio
		else:
			full_portfolio = full_portfolio.append(sector_portfolio)

	full_portfolio.to_csv(full_portfolio_path, index = False)
	return full_portfolio

# selling_price: a dataframe containing Ticker and Selling Price columns
def PortfolioReportGeneration(full_portfolio, selling_price, date_prefix):
	full_portfolio = utils.fill_df(full_portfolio, "selling_price", selling_price, "price", "ticker")
	full_portfolio.loc[:, 'return'] = full_portfolio.loc[:, 'selling_price'] / full_portfolio.loc[:, 'buying_price'] - 1
	full_portfolio.loc[full_portfolio['position'] == 'short', 'return'] *= -1
	# If selling_price of some stocks could not be sold, set return to -1
	full_portfolio['return'].fillna(-1, inplace = True)

	result_dict = {'sector':[], 'position':[], 'return':[], 'std':[]}
	return_list = full_portfolio.groupby(["sector", "position"]).apply(__portfolio_report_helper)

	for sector, position, weighted_return, weighted_std in return_list:
		result_dict['sector'].append(sector)
		result_dict['position'].append(position)
		result_dict['return'].append(weighted_return)
		result_dict['std'].append(weighted_std)

	return_list = full_portfolio.groupby(["sector"]).apply(__portfolio_report_helper)
	for sector, _, weighted_return, weighted_std in return_list:
		result_dict['sector'].append(sector)
		result_dict['position'].append("total")
		result_dict['return'].append(weighted_return)
		result_dict['std'].append(weighted_std)	

	portfolio_return = pandas.DataFrame(result_dict)

	portfolio_return.to_csv('./portfolio/return_report/%s_portfolio_return_report.csv'%date_prefix, index = False)
	
	return portfolio_return

def __portfolio_report_helper(divided_portfolio):
	first_sector = divided_portfolio['sector'].iloc[0]
	first_position = divided_portfolio['position'].iloc[0]
	# normalize weight
	divided_portfolio.loc[:, 'weight'] = divided_portfolio.loc[:, 'weight'] / divided_portfolio.loc[:, 'weight'].sum(skipna = True)
	weighted_return = (divided_portfolio.loc[:, 'return'] * divided_portfolio.loc[:, 'weight']).sum(skipna = True)
	weighted_std = np.sqrt((((divided_portfolio.loc[:, 'return'] - weighted_return)**2)*divided_portfolio.loc[:, 'weight']).sum(skipna = True))
	return (first_sector, first_position, weighted_return, weighted_std)

def StrategyPerformanceEvaluation(sectors, portfolio_dates, start_date, end_date, strategy_performance_period):
	portfolio_dates.set_index(keys = ['sell'], drop = False, inplace = True)
	period_start = start_date

	while period_start <= end_date:
		period_end = None
		if strategy_performance_period == "whole":
			period_end = end_date
		else:
			period_end = start_date + timedelta(days = strategy_performance_period)

		portfolio_build_dates = portfolio_dates['sell'].between(left = period_start, right = period_end)
		return_reports = []

		for date in portfolio_build_dates:
			date_prefix = date.strftime(config.date_format)
			return_reports.append(pandas.read_csv('./portfolio/return_report/%s_portfolio_return_report.csv'%date_prefix))
		full_return_reports = pandas.concat(return_reports, ignore_index = True)

		raw_strategy_report = generate_raw_strategy_report(full_return_reports)
		output_file_name = "[%s - %s]strategy_performance_report.xls"%(period_start.strftime(config.date_format), period_end.strftime(config.date_format))
		reformat_report(raw_strategy_report, output_file_name, sectors)

		period_start = period_end

def generate_raw_strategy_report(combined_reports):
	result_dict = {'sector':[], 'position':[], 'return':[], 'std':[]}

	return_list = combined_reports.groupby(["sector", "position"]).apply(__strategy_report_helper)
	for sector, position, sector_return, std in return_list:
		result_dict['sector'].append(sector)
		result_dict['position'].append(position)
		result_dict['return'].append(sector_return)
		result_dict['std'].append(std)

	return pandas.DataFrame(result_dict)

def __strategy_report_helper(divided_report):
	first_sector = divided_report['sector'].iloc[0]
	first_position = divided_report['position'].iloc[0]

	mean_return = divided_report["return"].mean(skipna = True)
	std = divided_report["return"].std(skipna = True)

	return (first_sector, first_position, mean_return, std)

