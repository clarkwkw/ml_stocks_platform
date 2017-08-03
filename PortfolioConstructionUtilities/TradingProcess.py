import heapq
import debug
import config
import json
from datetime import timedelta
import utils
import os
import numpy as np
import pandas
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay
from DataSource import DownloadTableFileFromMySQL, LoadTableFromFile
from ModelOperation import MachineLearningModelDevelopment
from PortfolioOperation import PortfolioConstruction, PortfolioReportGeneration, StrategyPerformanceEvaluation

def SimulateTradingProcess(simulation_config_dict, stock_data_config_dict):
	setup_dirs(stock_data_config_dict['stock_data_code'], simulation_config_dict['run_code'], stock_data_config_dict['sectors'])
	
	with open("run_config.json", "w") as f:
		f.write(json.dumps(simulation_config_dict, indent = 4))
	
	ML_sector_factors, price_info = None, None
	ML_factors_dir = "./model/sector_stock_dataset"
	if stock_data_config_dict['use_loaded_data']:
		ML_sector_factors, price_info = LoadTableFromFile(stock_data_config_dict['sectors'], ML_factors_dir)
	else:
		if not stock_data_config_dict['save_downloaded_data']:
			ML_factors_dir = None
		ML_sector_factors, price_info = DownloadTableFileFromMySQL(stock_data_config_dict['market_id'], stock_data_config_dict['sectors'], stock_data_config_dict['factors'], stock_data_config_dict['market_cap'], start_date = stock_data_config_dict['period']['start'], end_date = stock_data_config_dict['period']['end'], output_dir = ML_factors_dir)
	
	start_date = pandas.Timestamp(stock_data_config_dict['period']['start'])
	end_date = pandas.Timestamp(stock_data_config_dict['period']['end'])
	date_queue = Date_Queue(start_date, end_date, stock_data_config_dict['market_id'])
	date_queue.push(start_date + timedelta(days = simulation_config_dict["model_training_frequency"]))

	debug.log("TradingProcess: Starting simulation..")
	buy_dates = []
	sell_dates = []
	while not date_queue.is_empty():
		date, _ = date_queue.pop()
		debug.log("TradingProcess: Training model on %s.."%(date.strftime(config.date_format)))
		buy_date, sell_date = trade(ML_sector_factors, date_queue, date, simulation_config_dict, price_info)
		buy_dates.append(buy_date)
		sell_dates.append(sell_date)

	trading_dates = pandas.DataFrame({'buy': buy_dates, 'sell': sell_dates})
	StrategyPerformanceEvaluation(trading_dates, strategy_performance_period = simulation_config_dict['strategy_performance_period'])

def trade(ML_sector_factors, queue, cur_date, simulation_config_dict, price_info):
	# train_model
	filtered_factors = {}
	dataset_start_date = cur_date - timedelta(days = simulation_config_dict["portfolio_holding_period"])
	for sector in ML_sector_factors:
		raw_df = ML_sector_factors[sector]
		filtered_factors[sector] = raw_df.loc[(raw_df['date'] >= dataset_start_date) & (raw_df['date'] <= cur_date)].copy()
		filtered_factors[sector].is_copy = False

	para_tune_holding_flag, para_tune_data_split_date, para_tune_data_split_period = None, None, None
	if "para_tune_holding_flag" in simulation_config_dict:
		para_tune_holding_flag = simulation_config_dict["simulation_config_dict"]
	if "para_tune_data_split_date" in simulation_config_dict:
		para_tune_data_split_date = simulation_config_dict["para_tune_data_split_date"]
	if "para_tune_data_split_period" in simulation_config_dict:
		para_tune_data_split_period = simulation_config_dict["para_tune_data_split_period"]

	models_map = MachineLearningModelDevelopment(filtered_factors, simulation_config_dict["model_flag"], simulation_config_dict["meta_paras"], simulation_config_dict["stock_filter_flag"], simulation_config_dict["B_top"], simulation_config_dict["B_bottom"], simulation_config_dict["target_label_holding_period"], simulation_config_dict["trading_stock_quantity"], para_tune_holding_flag, period = para_tune_data_split_period, date = para_tune_data_split_date)

	# confirm next training date
	next_train_date = cur_date + timedelta(days = simulation_config_dict["model_training_frequency"])
	queue.push(next_train_date)

	# confirm portfolio buy and sell date
	build_date = queue.get_next_bday(cur_date)
	build_date_str = build_date.strftime(config.date_format)

	holding_end_date = queue.get_next_bday(build_date + timedelta(days = simulation_config_dict['portfolio_holding_period']))
	holding_end_date_str = holding_end_date.strftime(config.date_format)
	

	# only provide stocks that have price info on both days
	buying_prices = price_info.loc[price_info['date'] == build_date, ['ticker', 'price']]
	selling_prices = price_info.loc[price_info['date'] == holding_end_date, ['ticker', 'price']]
	buying_prices.to_csv("test_buying.csv")
	selling_prices.to_csv("test_selling.csv")
	tradable_tickers = pandas.Series(np.intersect1d(buying_prices['ticker'].values, selling_prices['ticker'].values))
	debug.log("TradingProcess: Tradable tickers: %d.."%tradable_tickers.size)

	buying_prices = buying_prices.loc[buying_prices['ticker'].isin(tradable_tickers)]
	selling_prices = selling_prices.loc[selling_prices['ticker'].isin(tradable_tickers)]

	# build portfolio and 'buy' stocks
	filtered_factors = {}
	for sector in ML_sector_factors:
		raw_df = ML_sector_factors[sector]
		filtered_factors[sector] = raw_df.loc[raw_df['date'] == build_date].copy()
		filtered_factors[sector].is_copy = False

	full_portfolio = PortfolioConstruction(filtered_factors, buying_prices, 10, simulation_config_dict['stock_filter_flag'], build_date_str, trained_models_map = models_map)

	# build portfolio and 'sell' stocks
	PortfolioReportGeneration(full_portfolio, selling_prices, holding_end_date_str)

	return (build_date, holding_end_date)

class Date_Queue:
	def __init__(self, start_date, end_date, market_id):
		self._business_days = get_business_days(market_id, start_date, end_date)
		self._queue = []
		self._cur_date = start_date
		self._start_date = start_date
		self._end_date = end_date

	def is_empty(self):
		return len(self._queue) == 0

	def push(self, date, paras_tup = None):
		if date < self._cur_date:
			raise Exception("Cannot schedule a task that happens in the past (cur_date = %s, date = %s)"%(self._cur_date.strftime(config.date_format), date.strftime(config.date_format)))
		if self._cur_date >= self._end_date:
			raise Expcetion("Cannot schedule a task that happens when it is currently at the end of the period")
		if date >= self._end_date:
			date = self._end_date

		heapq.heappush(self._queue, (date, paras_tup))

	def pop(self):
		date, paras_tup = heapq.heappop(self._queue)
		self._cur_date = date
		return date, paras_tup

	def get_next_bday(self, date):
		if type(date) is str:
			date = pandas.Timestamp(date)

		index = self._business_days.get_loc(date, "backfill")
		if date == self._business_days[index]:
			index += 1

		if index >= len(self._business_days):
			return None

		return self._business_days[index]

def get_business_days(area, start_date, end_date):
	if area == 'US':
		us_bd = CustomBusinessDay(calendar = USFederalHolidayCalendar())
		return pandas.DatetimeIndex(start = start_date, end = end_date, freq = us_bd)
	else:
		raise Exception("Unexpected value for area '%s'"%str(area))

def setup_dirs(stock_data_code = None, run_code = None, sectors = []):
	if stock_data_code is not None:
		utils.create_dir("./%s"%stock_data_code)
		utils.create_dir("./%s/%s"%(stock_data_code, run_code))
		os.chdir("./%s/%s"%(stock_data_code, run_code))

	utils.create_dir("./model")
	utils.create_dir("./model/preprocessing")
	utils.create_dir("./model/sector_stock_dataset")
	for sector in sectors:
		utils.create_dir("./model/%s"%sector)

	utils.create_dir("./portfolio")
	utils.create_dir("./portfolio/full_portfolio")
	utils.create_dir("./portfolio/return_report")
	utils.create_dir("./portfolio/predicted_value")
	utils.create_dir("./portfolio/ranked_stock")
	utils.create_dir("./portfolio/sector_portfolio")
	for sector in sectors:
		utils.create_dir("./portfolio/%s"%sector)