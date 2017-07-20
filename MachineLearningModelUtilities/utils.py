import abc, six
import numpy as np
import pandas
import random
import warnings

id_fields = ["date", "ticker", "record_id"]
def split_dataset(X, y, valid_portion = 0.2):
	if valid_portion > 1 or valid_portion < 0:
		raise Exception("portion for the validation set should be between 0 and 1.")
	n_sample = X.shape[0]
	indices = random.sample(list(range(n_sample)), n_sample)
	train_indices = indices[int(valid_portion*n_sample):]
	valid_indices = indices[0:int(valid_portion*n_sample)]
	return X[train_indices, ], y[train_indices], X[valid_indices, ], y[valid_indices]

def prediction_to_df(id_frame, predictions):
	result = pandas.concat([id_frame, pandas.DataFrame({"pred": predictions})], axis = 1)
	return result

def label_to_dist(labels):
	dists = []
	for label in labels:
		dist = [0, 0]
		dist[int(label)] = 1
		dists.append(dist)
	return np.asarray(dists)

def dist_to_label(dists):
	# Simplified softmax
	labels = 1.0/(np.exp(np.array(dists[:, 0] - dists[:, 1], dtype= np.float128))+ 1)
	return labels

def import_custom_module(customized_module_name, customized_module_dir):
	file, filename, desc = imp.find_module(customized_module_name, path = customized_module_dir)
	custom_module = imp.load_module(customized_module_name, file, filename, desc)
	return custom_module

def get_factors_from_df(df):
	factors = list(df)
	for id_field in id_fields:
		if id_field in factors:
			factors.remove(id_field)
	return factors

def seperate_factors_target(df):
	target = df['label'].as_matrix()
	factors = df.drop(['label', 'return'], 1)
	return factors, target

def raise_warning(msg):
	warnings.warn(msg, Warning)