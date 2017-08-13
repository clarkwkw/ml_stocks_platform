import math
import json

input_func = None

try:
	input_func = raw_input
except NameError:
	input_func = input

def save_dict(dict, path):
	with open(path, "w") as f:
		json.dump(dict, f, ensure_ascii = True, indent = 4)

def get_input_str(title, options = None, end = ':'):
	result = None
	options_str = ""
	if type(options) is list:
		options_str = " [%s]"%(", ".join(options))
	while True:
		result = input_func("%s%s%s "%(title, options_str, end))
		if type(options) is list and result not in options:
			print("Input must be one of the following:\n%s\nTry again."%options_str)
			continue
		break
	return result

def get_input_bool(title):
	tmp_result = get_input_str(title, options = ["y", "n"], end = "?")
	if tmp_result == "y":
		return True
	else:
		return False

def get_input_number(title, lower_limit = float("-inf"), upper_limit = float("inf"), is_int = False, end = ':'):
	result = None

	while True:
		result = input_func("%s%s "%(title, end))
		try:
			result = float(result)
			if result < lower_limit or result > upper_limit:
				raise ValueError()
			if is_int:
				if math.fabs(int(result) - result) > 1e-5:
					raise ValueError()
				result = int(result)
		except ValueError:
			if is_int:
				input_type = "an integer"
			else:
				input_type = "a number"
			print("Input must be %s between %.3f - %.3f, try again"%(input_type, lower_limit, upper_limit))
			continue
		break
	return result

def get_input_str_list(title, end = ":", separator = ","):
	result = input_func("%s (separate by '%s')%s "%(title, separator, end))
	result = [x.strip() for x in result.split(separator)]
	return result
