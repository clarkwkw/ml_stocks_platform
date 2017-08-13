import config
import argparse
import debug
import os
import importlib
import warnings
from timeit import default_timer as timer
from datetime import timedelta 
import DataPreparation

_test_scripts_dir = "test_scripts"

def arg_parse():
	parser = argparse.ArgumentParser()
	parser.add_argument("testscript", help = "testing script to call under ./%s"%_test_scripts_dir)
	parser.add_argument("-i", "--ignorewarnings", help = "ignore all warnings", action = "store_true")
	args = parser.parse_args()
	return args

def test(script):
	try:
		os.makedirs("./test_output")
	except OSError:
		pass

	try:
		m = importlib.import_module("%s.%s"%(_test_scripts_dir, script))
	except ImportError:
		print(">> Testing script '%s' not found, abort."%script)
		exit(-1)

	print(">> Testing Script: %s"%script)
	start = timer()
	m.test()
	end = timer()
	total_runtime = end - start
	prep_runtime = DataPreparation.runtime
	print(">> Execution time: {:02.0f}:{:02.0f}:{:05.2f}".format(total_runtime//3600, (total_runtime%3600)//60, total_runtime%60))
	print(">> Preparation Module: {:02.0f}:{:02.0f}:{:05.2f}".format(prep_runtime//3600, (prep_runtime%3600)//60, prep_runtime%60))


if __name__ == "__main__":
	args = arg_parse()
	if ignore_warn or args.ignore_warnings:
		warnings.simplefilter("ignore")
	test(args.testscript, args.ignorewarnings)