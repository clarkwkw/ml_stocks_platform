import abc, six
import json
import numpy as np
import pandas
import pickle
import random
from sklearn.svm import SVC
from sklearn.externals import joblib
import string
import tensorflow as tf

_multi_thread = 8

def cal_cross_entropy(predict, real):
	return tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels=real, logits=predict))

def split_dataset(X, y, valid_portion = 0.2):
	if valid_portion > 1 or valid_portion < 0:
		raise Exception("portion for the validation set should be between 0 and 1.")
	n_sample = X.shape[0]
	indices = random.sample(list(range(n_sample)), n_sample)
	train_indices = indices[int(valid_portion*n_sample):]
	valid_indices = indices[0:int(valid_portion*n_sample)]
	return X[train_indices, ], y[train_indices], X[valid_indices, ], y[valid_indices]

def prediction_to_df(dates, predictions):
	data = {'date': dates, 'label': predictions}
	result = pandas.DataFrame(data, index = dates, columns = ['date', 'label'])
	return result

def label_to_dist(labels):
	dists = []
	for label in labels:
		dist = [0, 0]
		dist[label] = 1
		dists.append(dist)
	return np.asarray(dists)


def dist_to_label(dists):
	labels = []
	for i in range(dists.shape[0]):
		if dists[i, 0] > dists[i, 1]:
			labels.append(0)
		else:
			labels.append(1)
	return np.asarray(labels)

@six.add_metaclass(abc.ABCMeta)
class GenericMLModel(object):

	@abc.abstractmethod
	def __init__(self, _factors = None, **kwargs):
		self._model = None
		self._trained = False
		self._factors = _factors

	def json_dict(self):
		dict = {'_factors': self._factors}
		return dict

	@abc.abstractmethod
	def train(self, machine_learning_factors, labels, **kwargs):
		pass

	@abc.abstractmethod
	def predict(self, machine_learning_factors, **kwargs):
		pass

	@abc.abstractmethod
	def save(self, savefile):
		pass

	@abc.abstractmethod
	def load(savefile):
		pass

	@property
	def trained(self):
		return self._trained

	def _parse_raw_df(self, raw_df):
		df = raw_df.drop(["date", "sector", "ticker", "record_id"], 1)
		raw_factors = list(df)
		if self._factors is not None:
			if len(raw_factors) != len(self._factors):
				raise Exception("No. of columns to parse [%d] does not match with self._factors [%d]."%(len(raw_factors), len(self._factors)))
		else:
			self._factors = raw_factors
		parsed_matrix = df.as_matrix(self._factors)
		dates = raw_df['date'].as_matrix()
		return (parsed_matrix, dates)

class SimpleSVMModel(GenericMLModel):
	def __init__(self, _factors = None, **kwargs):
		super(self.__class__, self).__init__(_factors)
		self._model = SVC(**kwargs)

	def train(self, machine_learning_factors, labels, **kwargs):
		if self._trained:
			raise Exception("Model already trained.")
		parsed_matrix, _ = self._parse_raw_df(machine_learning_factors)
		self._model.fit(parsed_matrix, labels, **kwargs)
		self._trained = True

	def predict(self, machine_learning_factors, **kwargs):
		if not self._trained:
			raise Exception("Model not trained.")
		parsed_matrix, dates = self._parse_raw_df(machine_learning_factors)
		predictions = self._model.predict(parsed_matrix, **kwargs)
		return prediction_to_df(dates, predictions)

	def save(self, savepath):
		if not self._trained:
			raise Exception("Model not trained.")
		joblib.dump(self._model, savepath+'/simplesvm.model')
		json_dict = {'_factors': self._factors}
		with open(savepath+'/simplesvm.conf', "w") as f:
			json.dump(json_dict, f)

	@staticmethod
	def load(savepath):
		with open(savepath+"/simplesvm.conf", "r") as f:
			json_dict = json.load(f)
		model = SimpleSVMModel(**json_dict)
		model._model = joblib.load(savepath+"/simplesvm.model")
		model._trained = True
		return model

class SimpleNNModel(GenericMLModel):
	def __init__(self, _factors = None, _hidden_nodes = [], _learning_rate = 0.001, **kwargs):
		super(self.__class__, self).__init__(_factors)
		self._hidden_nodes = _hidden_nodes
		self._weights = []
		self._biases = []
		self._sess = None
		self._learning_rate = 0.001
		self._scope_name = tf_scope_manager.register()
		with tf.variable_scope(self._scope_name) as scope:
			n_last_layer = len(self._factors)
			n_next_layer = 0
			for i in range(0, len(_hidden_nodes) + 1):
				if i >= len(_hidden_nodes):
					n_next_layer = 2
				else:
					n_next_layer = _hidden_nodes[i]
				self._weights.append(tf.get_variable("w_"+str(i), initializer = tf.random_normal([n_last_layer, n_next_layer])))
				self._biases.append(tf.get_variable("b_"+str(i), initializer = tf.random_normal([n_next_layer])))
				if i < len(_hidden_nodes):
					n_last_layer = _hidden_nodes[i]
			self._X = tf.placeholder(tf.float32, [None, len(self._factors)])
			self._y = tf.placeholder(tf.float32, [None, 2])
			self._pred = self.__network(self._X)
			self._cost = cal_cross_entropy(self._pred, self._y)
			self._optimizer = tf.train.AdamOptimizer(self._learning_rate).minimize(self._cost)
			init = tf.global_variables_initializer()
			self._sess = tf.Session(config=tf.ConfigProto(intra_op_parallelism_threads = _multi_thread))
			self._sess.run(init)	
		
	def train(self, machine_learning_factors, labels, learning_rate = 0.001, adaptive = True, step = 300, max_iter = 10000, **kwargs):
		if self._trained:
			raise Exception("Model already trained.")
		parsed_matrix, _ = self._parse_raw_df(machine_learning_factors)
		
		if adaptive:
			train_X, train_y, valid_X, valid_y = split_dataset(parsed_matrix, labels)
			valid_y = label_to_dist(valid_y)
			valid_y = tf.constant(valid_y)
			decider = Train_decider()
		else:
			train_X, train_y = parsed_matrix, labels
		train_y = label_to_dist(train_y)
		for i in range(max_iter):
			_, train_cost = self._sess.run([self._optimizer, self._cost], feed_dict = {self._X: train_X, self._y: train_y})
			if adaptive and (i+1)%step == 0:
				valid_predict = self._pred.eval(feed_dict = {self._X: valid_X}, session = self._sess)
				valid_cost = cal_cross_entropy(valid_predict, valid_y).eval(session = self._sess)
				#print("Epoch %5d: %.4f"%(i+1, valid_cost))
				if decider.update(valid_cost) == False:
					break
		self._trained = True

	def predict(self, machine_learning_factors, **kwargs):
		if not self._trained:
			raise Exception("Model not trained.")
		parsed_matrix, dates = self._parse_raw_df(machine_learning_factors)
		tmp_result = self._pred.eval(feed_dict = {self._X: parsed_matrix}, session = self._sess)
		tmp_result = dist_to_label(tmp_result)
		return prediction_to_df(dates, tmp_result)

	def __network(self, X):
			tmp_result = X
			for i in range(len(self._hidden_nodes) + 1):
				tmp_result = tf.add(tf.matmul(tmp_result, self._weights[i]), self._biases[i])
				if i < len(self._hidden_nodes):
					tmp_result = tf.nn.sigmoid(tmp_result)
			return tmp_result

	def save(self, savepath):
		if not self._trained:
			raise Exception("Model not trained.")
		tf.train.export_meta_graph(savepath+"/simplenn.model", export_scope = self._scope_name)
		json_dict = {'_factors': self._factors, '_hidden_nodes': self._hidden_nodes}
		with open(savepath+'/simplenn.conf', "w") as f:
			json.dump(json_dict, f)
		
	@staticmethod
	def load(savepath):
		with open(savepath+"/simplenn.conf", "r") as f:
			json_dict = json.load(f)
		model = SimpleNNModel(**json_dict)
		tf.train.import_meta_graph(savepath+"/simplenn.model", import_scope = model._scope_name)
		model._trained = True
		return model

class Train_decider:
	tolerance = 2

	def __init__(self):
		self.cost_initialized = False
		self.cont = True
		self.count = 0
	
	# Given a error cost, compare it with the previous one, if it keeps increasing for certain no. of calls, return False.
	# Otherwise, return True
	def update(self, cost):
		if self.cost_initialized == False:
			self.prev_cost = cost
			self.cost_initialized = True
			self.cont = True
			self.count = 0
		else:
			if cost > self.prev_cost:
				self.count = self.count + 1
			else:
				self.count = 0
			self.prev_cost = cost
			self.cont = self.count <= Train_decider.tolerance
		return self.cont

	def cont(self):
		return self.cont

class TFScopeManager:
	def __init__(self):
		self.scopes = {}

	def register(self):
		name = self.__random_name()
		while name in self.scopes:
			name = self.__random_name()
		self.scopes[name] = True
		return name

	def __random_name(self, name_length = 6, chars = string.ascii_uppercase + string.digits):
		return ''.join(random.choice(chars) for _ in range(name_length))

tf_scope_manager = TFScopeManager()