import config
import errno
import json
import os
import warnings
import numpy as np
import sqlalchemy

_id_fields = ['record_id', 'date', 'ticker', 'sector']
_necessary_factors = ['last_price', 'volume']

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

def fill_df(target_df, target_column, src_df, src_column, id_column):
	def fun(row):
		id_value = row[id_column]
		fill_values = src_df.loc[src_df[id_column] == id_value, src_column]
		if fill_values.size > 0:
			row[target_column] = fill_values.iloc[0]
		return row
	target_df[target_column] = np.NAN
	target_df = target_df.apply(fun, axis = 1)
	return target_df

def get_mysql_engine():
	try:
		engine = sqlalchemy.create_engine('mysql+%s://%s:%s@%s/%s'%(config.sql_db_connector, config.sql_username, config.sql_password, config.sql_host, config.sql_database))
		conn = engine.connect()
		conn.close()
	except sqlalchemy.exc.OperationalError as e:
		raise Exception('Fail to connect to MYSQL database')
	return engine