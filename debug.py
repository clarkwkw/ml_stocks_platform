from __future__ import print_function
import config
import sys

def log(msg, f = sys.stdout):
	if not config.debug_log:
		return

	lines = msg.split("\n")
	for line in lines:
		print("> %s"%line, file = f)