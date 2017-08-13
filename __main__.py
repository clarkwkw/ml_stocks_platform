# file mlsp/__main__.py

import config
import argparse
import debug
import json
import os
import pandas
import importlib
import warnings
import PortfolioConstructionUtilities as PortfolioUtils
import ConfigFilesUtilities
try:
	import testscripts
	test_included = True
except ImportError:
	test_included = False

def parse_args():
	parser = argparse.ArgumentParser()

	parser.add_argument("-i", "--ignorewarnings", help = "ignore all warnings", action = "store_true")

	action_group = parser.add_mutually_exclusive_group(required = False)
	if test_included:
		action_group.add_argument("-t", "--testscript", help = "execute test script install_path/%s"%testscripts._test_scripts_dir)
	action_group.add_argument("-f", "--formatreports", nargs = "+", help = "convert raw portfolio return report(s) / strategy performance report(s) into formatted xls file(s)")
	action_group.add_argument("-s", "--strategyreport", nargs = "+", help = "convert raw portfolio return report(s) into a strategy performance report (xls file)")
	action_group.add_argument("-g", "--generateconfig", choices = ConfigFilesUtilities.config_files, help = "which config file to generate")

	parser.add_argument("-o", "--output", help = "[-s/ --strategyreport] name of the output file")

	args = parser.parse_args()
	if args.strategyreport is not None and args.output is None:
		raise argparse.ArgumentTypeError("argument -o/--output: expected a output file name")

	return args

def main():
	args = parse_args()

	if config.ignore_warnings or args.ignore_warnings:
		warnings.simplefilter("ignore")

	if test_included and args.testscript is not None:
		testscripts.test(args.testscript)
		
	elif args.generateconfig is not None:
		if args.generateconfig == "simulation":
			ConfigFilesUtilities.generate_simulation_config()
		elif args.generateconfig == "stock_data":
			ConfigFilesUtilities.generate_stock_data_config()
		else:
			raise Exception("Unexpected config file name '%s'"%args.generateconfig)

	elif args.formatreports is not None:
		max_len = 0
		for raw_report_path in args.formatreports:
			max_len = max(max_len, len(raw_report_path))

		for raw_report_path in args.formatreports:
			folder = os.path.dirname(raw_report_path)
			file_name = os.path.splitext(os.path.basename(raw_report_path))[0]
			target_path = "%s.xls"%file_name
			try:
				PortfolioUtils.reformat_report(raw_report_path, target_path)
				print("%-*s  ->  %s"%(max_len, raw_report_path, target_path))
			except IOError:
				print("%-*s  ->  [error opening raw report]"%(max_len, raw_report_path))

	elif args.strategyreport is not None:
		return_reports = []
		for path in args.strategyreport:
			try:
				return_reports.append(pandas.read_csv(path))
			except IOError:
				print("error opening return report %s"%path)
				exit(-1)
		
		combined_return_report = pandas.concat(return_reports, ignore_index = True)
		raw_strategy_report = PortfolioUtils.generate_raw_strategy_report(combined_return_report)
		PortfolioUtils.reformat_report(raw_strategy_report, args.output)

	else:
		simulation_config_dict, stock_data_config_dict = None, None
		try:
			with open("stock_data_config.json", "r") as f:
				stock_data_config_dict = json.load(f)
				stock_data_code = stock_data_config_dict["stock_data_code"]
		except IOError:
			print("cannot open stock_data_config.json, abort")
			exit(-1)

		try:
			with open("./%s/simulation_config.json"%stock_data_config_dict['stock_data_code'], "r") as f:
				simulation_config_dict = json.load(f)
		except:
			print("cannot open %s/simulation_config.json, abort")
			exit(-1)

		PortfolioUtils.SimulateTradingProcess(simulation_config_dict, stock_data_config_dict)

if __name__ == "__main__":
	main()