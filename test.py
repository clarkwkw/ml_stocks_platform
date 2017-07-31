import config
import argparse
import debug
import os
import importlib
import warnings

_test_scripts_dir = "test_scripts"

parser = argparse.ArgumentParser()
parser.add_argument("testscript", help = "Testing script to call under ./%s"%_test_scripts_dir)
parser.add_argument("-i", "--ignorewarnings", help = "Ignore all warnings.", action = "store_true")


args = parser.parse_args()

try:
	m = importlib.import_module("%s.%s"%(_test_scripts_dir, args.testscript))
except ImportError:
	print(">> Testing script '%s' not found, abort."%args.testscript)
	exit(-1)


if args.ignorewarnings or config.ignore_warnings:
	warnings.simplefilter("ignore")

try:
	os.makedirs("./test_output")
except OSError:
	pass

print(">> Testing Script: %s"%args.testscript)

m.test()