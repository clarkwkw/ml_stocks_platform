import errno
import json
import os
import warnings
import numpy as np
import sqlalchemy

_id_fields = ['record_id', 'date', 'ticker', 'sector']

class DateQueue:
	def __init__(self, interval = 365):
		self.queue = []
		self.interval = interval
	def pop(self, cur_date):
		queue = self.queue
		while len(queue) > 1 and (cur_date - queue[1][0]).days >= self.interval and (cur_date - queue[0][0]).days >= self.interval:
			queue.pop(0)
		if len(queue) == 0 or (cur_date - queue[0][0]).days < self.interval:
			return None
		return queue[0]
	def push(self, date, val):
		self.queue.append((date, val))

def raise_warning(msg):
	warnings.warn(msg, Warning)

def set_equal_weights(stocks_df, selected_indices):
	n_stocks = np.sum(selected_indices)
	stocks_df.loc[selected_indices, 'weight'] = 1.0/n_stocks
	return stocks_df

def cwd(directory):
	os.chdir(directory)

def create_dir(path):
	try:
		os.makedirs(path)
	except OSError as exception:
		if exception.errno != errno.EEXIST:
			raise

def fill_df(target_df, src_df, id_column, fill_column):
	def fun(row):
		id_value = row[id_column]
		fill_value = src_df.loc[src_df[id_column] == id_value, fill_column].iloc[0]
		row[fill_column] = fill_value
		return row
	target_df[fill_column] = np.NAN
	target_df = target_df.apply(fun, axis = 1)
	return target_df

def get_mysql_engine():
	config_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
	config = None
	with open(config_dir+"/mysql.json", "r") as f:
		config = json.load(f)
	try:
		engine = sqlalchemy.create_engine('mysql+%s://%s:%s@%s/%s'%(config["db_connector"], config["username"], config["password"], config["host"], config["database"]))
		conn = engine.connect()
		conn.close()
	except sqlalchemy.exc.OperationalError as e:
		raise Exception('Fail to connect to MYSQL database')
	return engine