import config
import argparse
import debug
import os
import importlib
import warnings

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

	m.test()

if __name__ == "__main__":
	args = arg_parse()
	if ignore_warn or args.ignore_warnings:
		warnings.simplefilter("ignore")
	test(args.testscript, args.ignorewarnings)

	


	