import argparse
import os
import importlib

_test_scripts_dir = "test_data"

parser = argparse.ArgumentParser()
parser.add_argument("testscript", help = "Testing script to call under ./%s"%_test_scripts_dir)

args = parser.parse_args()

try:
	m = importlib.import_module("%s.%s"%(_test_scripts_dir, args.testscript))
except ImportError:
	print(">> Testing script '%s' not found, abort."%args.testscript)
	exit(-1)

try:
	os.makedirs("./test_output")
except OSError:
	pass

print(">> Testing Script: %s"%args.testscript)

m.test()