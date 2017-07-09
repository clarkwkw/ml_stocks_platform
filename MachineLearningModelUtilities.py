import abc, six
import pandas
import pickle
from sklearn.svm import SVC
from sklearn.externals import joblib

@six.add_metaclass(abc.ABCMeta)
class GenericMLModel(object):
	@abc.abstractmethod
	def __init__(self, **kwargs):
		self._model = None
		self._trained = False
		self._colnames = None
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
	@staticmethod
	def parse_raw_df(raw_df, colnames = None):
		df = raw_df.drop(["date", "sector", "ticker", "record_id"], 1)
		raw_colnames = list(df)
		if colnames is not None:
			if len(raw_colnames) != len(colnames):
				raise Exception("No. of columns for prediction [%d] does not match with training data [%d]."%(len(raw_colnames), len(colnames)))
		else:
			colnames = raw_colnames
		X = df.as_matrix(colnames)
		dates = raw_df['date'].as_matrix()
		return (X, dates, colnames)
	@staticmethod
	def prediction_to_df(dates, predictions):
		data = {'date': dates, 'label': predictions}
		result = pandas.DataFrame(data, index = dates, columns = ['date', 'label'])
		return result

class SimpleSVMModel(GenericMLModel):
	def __init__(self, **kwargs):
		super(self.__class__, self).__init__()
		self._model = SVC(**kwargs)
	def train(self, machine_learning_factors, labels, **kwargs):
		if self._trained:
			raise Exception("Model already trained.")
		X, _, self._colnames = GenericMLModel.parse_raw_df(machine_learning_factors)
		self._model.fit(X, labels, **kwargs)
		self._trained = True
	def predict(self, machine_learning_factors, **kwargs):
		if not self._trained:
			raise Exception("Model not trained.")
		X, dates, self._colnames = GenericMLModel.parse_raw_df(machine_learning_factors, self._colnames)
		predictions = self._model.predict(X, **kwargs)
		return GenericMLModel.prediction_to_df(dates, predictions)
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
	def __init__(self, **kwargs):
		super(self.__class__, self).__init__()
	def train(self, machine_learning_factors, labels, **kwargs):
		if self._trained:
			raise Exception("Model already trained.")
		X, _, self._colnames = GenericMLModel.parse_raw_df(machine_learning_factors)
		# todo: add training code
		self._trained = True
	def predict(self, machine_learning_factors, **kwargs):
		if not self._trained:
			raise Exception("Model not trained.")
		X, dates, self._colnames = GenericMLModel.parse_raw_df(machine_learning_factors, self._colnames)
		# todo: add prediction code
		return GenericMLModel.prediction_to_df(dates, predictions)
	def save(self, savefile):
		if not self._trained:
			raise Exception("Model not trained.")
		# todo: add saving code
	def load(self, savefile):
		if self._trained:
			raise Exception("Model already trained.")
		# todo: add loading code
		self._trained = True