import json
from GenericMLModel import GenericMLModel
from utils import *
import tensorflow as tf

_multi_thread = 8

def cal_cross_entropy(predict, real):
	return tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(labels=real, logits=predict))

class SimpleNNModel(GenericMLModel):
	def __init__(self, _factors = None, _hidden_nodes = [], _learning_rate = 0.001, from_save = None, **kwargs):
		super(self.__class__, self).__init__(_factors)
		self._hidden_nodes = _hidden_nodes
		self._weights = []
		self._biases = []		
		self._learning_rate = 0.001
		self._graph = tf.Graph()
		self._sess = tf.Session(graph = self._graph, config=tf.ConfigProto(intra_op_parallelism_threads = _multi_thread))
		with self._graph.as_default() as g:
			if from_save is None:
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
				self._X = tf.placeholder(tf.float32, [None, len(self._factors)], name = "X")
				self._y = tf.placeholder(tf.float32, [None, 2], name = "y")
				self._pred = self.__network(self._X)
				tf.add_to_collection("pred", self._pred)
				self._cost = cal_cross_entropy(self._pred, self._y)
				self._optimizer = tf.train.AdamOptimizer(self._learning_rate).minimize(self._cost)
				init = tf.global_variables_initializer()
				self._sess.run(init)	
			else:
				from_save = from_save.rstrip('/')
				saver = tf.train.import_meta_graph(from_save+'/simplenn.ckpt.meta')
				saver.restore(self._sess, from_save+'/simplenn.ckpt')
				self._X = g.get_tensor_by_name("X:0")
				self._y = g.get_tensor_by_name("y:0")
				self._pred =  tf.get_collection("pred")[0]
		tf.reset_default_graph()

	def train(self, machine_learning_factors, labels, adaptive = True, step = 300, max_iter = 10000, **kwargs):
		if self._trained:
			raise Exception("Model already trained.")
		parsed_matrix, _ = self._parse_raw_df(machine_learning_factors)
		with self._graph.as_default() as g:
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
		tf.reset_default_graph()
		self._trained = True

	def predict(self, machine_learning_factors, **kwargs):
		if not self._trained:
			raise Exception("Model not trained.")
		parsed_matrix, id_frame = self._parse_raw_df(machine_learning_factors)
		with self._graph.as_default() as g:
			tmp_result = self._sess.run(self._pred, feed_dict = {self._X: parsed_matrix})
			tmp_result = dist_to_label(tmp_result)
		tf.reset_default_graph()
		return self._prediction_to_df(id_frame, tmp_result)

	def __network(self, X):
		tmp_result = X
		for i in range(len(self._hidden_nodes) + 1):
			tmp_result = tf.add(tf.matmul(tmp_result, self._weights[i]), self._biases[i])
			if i < len(self._hidden_nodes):
				tmp_result = tf.nn.sigmoid(tmp_result)
		return tmp_result

	def save(self, savedir):
		if not self._trained:
			raise Exception("Model not trained.")
		with self._graph.as_default() as g:
			saver = tf.train.Saver()
			save_path = saver.save(self._sess, save_path = savedir+'/simplenn.ckpt')
		tf.reset_default_graph()
		json_dict = {	'model_type': 'NN', 
						'init_paras': 
									{	'_factors': self._factors, 
										'_hidden_nodes': self._hidden_nodes
									}
					}
		with open(savedir+'/model.conf', "w") as f:
			json.dump(json_dict, f)
		
	@staticmethod
	def load(savedir):
		with open(savedir+"/model.conf", "r") as f:
			json_dict = json.load(f)
		model = SimpleNNModel(from_save = savedir, **json_dict["init_paras"])
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