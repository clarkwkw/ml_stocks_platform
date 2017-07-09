import abc, six
import numpy as np
import pandas
import pickle
import random
from sklearn.svm import SVC
from sklearn.externals import joblib
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
	def __init__(self, factors = None, **kwargs):
		self._model = None
		self._trained = False
		self._factors = factors

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
	def load(self, savefile):
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
	def __init__(self, factors = None, **kwargs):
		super(self.__class__, self).__init__(factors)
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

	def save(self, savefile):
		if not self._trained:
			raise Exception("Model not trained.")
		joblib.dump(self._model, savefile)

	def load(self, savefile):
		if self._trained:
			raise Exception("Model already trained.")
		self._model = joblib.load(savefile)
		self._trained = True

class SimpleNNModel(GenericMLModel):
	def __init__(self, factors = None, hidden_nodes = [], **kwargs):
		super(self.__class__, self).__init__(factors)
		self._hidden_nodes = hidden_nodes
		self._weights = []
		self._biases = []
		n_last_layer = len(self._factors)
		n_next_layer = 0
		for i in range(0, len(hidden_nodes) + 1):
			if i >= len(hidden_nodes):
				n_next_layer = 2
			else:
				n_next_layer = hidden_nodes[i]
			self._weights.append(tf.Variable(tf.random_normal([n_last_layer, n_next_layer]), name = "w_"+str(i)))
			self._biases.append(tf.Variable(tf.random_normal([n_next_layer]), name = "b_"+str(i)))
			if i < len(hidden_nodes):
				n_last_layer = hidden_nodes[i]

	def train(self, machine_learning_factors, labels, learning_rate = 0.001, adaptive = True, step = 300, max_iter = 10000, **kwargs):
		if self._trained:
			raise Exception("Model already trained.")

		parsed_matrix, _ = self._parse_raw_df(machine_learning_factors)
		X = tf.placeholder(tf.float32, [None, len(self._factors)])
		Y = tf.placeholder(tf.float32, [None, 2])
		pred = self.__network(X)
		cost = cal_cross_entropy(pred, Y)
		optimizer = tf.train.AdamOptimizer(learning_rate).minimize(cost)
		init = tf.global_variables_initializer()
		sess = tf.Session(config=tf.ConfigProto(intra_op_parallelism_threads = _multi_thread))
		sess.run(init)		
		if adaptive:
			train_X, train_y, valid_X, valid_y = split_dataset(parsed_matrix, labels)
			valid_y = label_to_dist(valid_y)
			valid_y = tf.constant(valid_y)
			decider = Train_decider()
		else:
			train_X, train_y = parsed_matrix, labels
		train_y = label_to_dist(train_y)
		for i in range(max_iter):
			_, train_cost = sess.run([optimizer, cost], feed_dict = {X: train_X, Y: train_y})
			if adaptive and (i+1)%step == 0:
				valid_predict = pred.eval(feed_dict = {X:valid_X}, session = sess)
				valid_cost = cal_cross_entropy(valid_predict, valid_y).eval(session = sess)
				print("Epoch %5d: %.4f"%(i+1, valid_cost))
				if decider.update(valid_cost) == False:
					break

		self._trained = True

	def predict(self, machine_learning_factors, **kwargs):
		if not self._trained:
			raise Exception("Model not trained.")
		parsed_matrix, dates = self._parse_raw_df(machine_learning_factors)
		X = tf.placeholder(tf.float32, [None, len(self._factors)])
		Y = tf.placeholder(tf.float32, [None, 2])
		pred = self.__network(X)
		init = tf.global_variables_initializer()
		sess = tf.Session(config=tf.ConfigProto(intra_op_parallelism_threads = _multi_thread))
		sess.run(init)
		tmp_result = pred.eval(feed_dict = {X: parsed_matrix}, session = sess)
		tmp_result = dist_to_label(tmp_result)
		return prediction_to_df(dates, tmp_result)

	def __network(self, X):
			tmp_result = X
			for i in range(len(self._hidden_nodes) + 1):
				tmp_result = tf.add(tf.matmul(tmp_result, self._weights[i]), self._biases[i])
				if i < len(self._hidden_nodes):
					tmp_result = tf.nn.sigmoid(tmp_result)
			return tmp_result

	def save(self, savefile):
		if not self._trained:
			raise Exception("Model not trained.")
		# todo: add saving code

	def load(self, savefile):
		if self._trained:
			raise Exception("Model already trained.")
		# todo: add loading code
		self._trained = True

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