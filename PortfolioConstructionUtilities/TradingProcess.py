import heapq
import debug
import config
import json
from datetime import timedelta
from dateutil.relativedelta import relativedelta
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
	ML_factors_dir = ".."
	if stock_data_config_dict['use_loaded_data']:
		ML_sector_factors, price_info = LoadTableFromFile(stock_data_config_dict['sectors'], ML_factors_dir)
	else:
		if not stock_data_config_dict['save_downloaded_data']:
			ML_factors_dir = None
		market_cap, include_null_cap = None, False
		if 'market_cap' in stock_data_config_dict:
			market_cap = stock_data_config_dict['market_cap']
			include_null_cap = stock_data_config_dict['include_null_cap']
		ML_sector_factors, price_info = DownloadTableFileFromMySQL(stock_data_config_dict['market_id'], stock_data_config_dict['sectors'], stock_data_config_dict['factors'], market_cap, include_null_cap, start_date = stock_data_config_dict['period']['start'], end_date = stock_data_config_dict['period']['end'], output_dir = ML_factors_dir)
	
	start_date = pandas.Timestamp(stock_data_config_dict['period']['start'])
	end_date = pandas.Timestamp(stock_data_config_dict['period']['end'])
	date_queue = Date_Queue(start_date, end_date, stock_data_config_dict['market_id'])
	if simulation_config_dict['reserve_train_data'] == 0:
		date_queue.push(start_date + timedelta(days = simulation_config_dict["model_training_frequency"]))
	else:
		date_queue.push(start_date + relativedelta(years = simulation_config_dict['reserve_train_data']))

	debug.log("TradingProcess: Starting simulation..")
	buy_dates = []
	sell_dates = []
	while not date_queue.is_empty():
		date, _ = date_queue.pop()
		debug.log("TradingProcess: Training model on %s.."%(date.strftime(config.date_format)))
		result = trade(ML_sector_factors, date_queue, date, simulation_config_dict, price_info)
		if type(result) is tuple:
			buy_dates.append(result[0])
			sell_dates.append(result[1])

	trading_dates = pandas.DataFrame({'buy': buy_dates, 'sell': sell_dates})
	trading_dates.to_csv("trading_dates.csv", index = False)
	StrategyPerformanceEvaluation(stock_data_config_dict['sectors'], trading_dates, start_date = stock_data_config_dict['period']['start'], end_date = stock_data_config_dict['period']['end'], strategy_performance_period = simulation_config_dict['strategy_performance_period'])

def trade(ML_sector_factors, queue, cur_date, simulation_config_dict, price_info):
	# confirm portfolio buy and sell date
	build_date = queue.get_next_bday(cur_date, False)
	
	if build_date is None:
		return

	holding_end_date = queue.get_next_bday(build_date + timedelta(days = simulation_config_dict['portfolio_holding_period']), inclusive = False)
	
	if holding_end_date is None:
		holding_end_date = queue.get_prev_bday(build_date + timedelta(days = simulation_config_dict['portfolio_holding_period']), inclusive = False)
		if build_date >= holding_end_date:
			return

	# train_model
	filtered_factors = {}
	dataset_start_date = cur_date - timedelta(days = simulation_config_dict["portfolio_holding_period"])
	for sector in ML_sector_factors:
		raw_df = ML_sector_factors[sector]
		if not simulation_config_dict["rolling_training_data"]:
			filtered_factors[sector] = raw_df.loc[(raw_df['date'] >= dataset_start_date) & (raw_df['date'] <= cur_date)].copy()
		else:
			filtered_factors[sector] = raw_df.loc[raw_df['date'] <= cur_date].copy()
		filtered_factors[sector].is_copy = False

	para_tune_holding_flag, para_tune_data_split_period = None, None
	if "para_tune_holding_flag" in simulation_config_dict:
		para_tune_holding_flag = simulation_config_dict["para_tune_holding_flag"]
	if "para_tune_data_split_period" in simulation_config_dict:
		para_tune_data_split_period = simulation_config_dict["para_tune_data_split_period"]

	model_dir, model_name = None, None
	if "custom_model_name" in simulation_config_dict:
		model_dir = "../../CustomModels"
		model_name = simulation_config_dict["custom_model_name"]
	models_map = MachineLearningModelDevelopment(filtered_factors, simulation_config_dict["model_flag"], simulation_config_dict["meta_paras"], simulation_config_dict["stock_filter_flag"], simulation_config_dict["B_top"], simulation_config_dict["B_bottom"], simulation_config_dict["target_label_holding_period"], simulation_config_dict["trading_stock_quantity"], para_tune_holding_flag, period = para_tune_data_split_period, customized_module_dir = model_dir, customized_module_name = model_name)

	# confirm next training date
	next_train_date = cur_date + timedelta(days = simulation_config_dict["model_training_frequency"])
	queue.push(next_train_date)

	build_date_str = build_date.strftime(config.date_format)
	holding_end_date_str = holding_end_date.strftime(config.date_format)
	
	# build portfolio and 'buy' stocks
	buying_prices = price_info.loc[price_info['date'] == build_date, ['ticker', 'price']]
	filtered_factors = {}
	for sector in ML_sector_factors:
		raw_df = ML_sector_factors[sector]
		filtered_factors[sector] = raw_df.loc[raw_df['date'] == build_date].copy()
		filtered_factors[sector].is_copy = False

	full_portfolio = PortfolioConstruction(filtered_factors, buying_prices, 10, simulation_config_dict['stock_filter_flag'], build_date_str, trained_models_map = models_map)

	# build portfolio and 'sell' stocks
	selling_prices = price_info.loc[price_info['date'] == holding_end_date, ['ticker', 'price']]
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
			utils.raise_warning("Cannot schedule a task that happens when it is currently at the end of the period")
			return
		if date >= self._end_date:
			date = self._end_date

		heapq.heappush(self._queue, (date, paras_tup))

	def pop(self):
		date, paras_tup = heapq.heappop(self._queue)
		self._cur_date = date
		return date, paras_tup

	def get_prev_bday(self, date, inclusive):
		if type(date) is str:
			date = pandas.Timestamp(date)

		try:
			index = self._business_days.get_loc(date, "ffill")
		except KeyError:
			return None

		if not inclusive and date == self._business_days[index]:
			index -= 1

		if index < 0:
			return None

		return self._business_days[index]

	def get_next_bday(self, date, inclusive):
		if type(date) is str:
			date = pandas.Timestamp(date)

		try:
			index = self._business_days.get_loc(date, "backfill")
		except KeyError:
			return None

		if not inclusive and date == self._business_days[index]:
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