import os
import pandas
import PortfolioConstructionUtilities as PortfolioUtils

def test():
	trading_dates = pandas.read_csv("test_data/trading_dates.csv", parse_dates = ["buy", "sell"])
	os.chdir("./test_output")
	PortfolioUtils.StrategyPerformanceEvaluation(["Materials", "Energy"], trading_dates, start_date = "2000-01-01", end_date = "2010-12-31", strategy_performance_period = 12)