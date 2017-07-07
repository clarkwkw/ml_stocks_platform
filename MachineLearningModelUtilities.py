from abc import ABCMeta, abstractmethod
import pandas
import pickle
from sklearn.svm import SVC
from sklearn.externals import joblib

class GenericMLModel(metaclass = ABCMeta):
	@abstractmethod
	def __init__(self, **kwargs):
		pass
	@abstractmethod
	def train(self, machine_learning_factors, labels, **kwargs):
		pass
	@abstractmethod
	def predict(self, machine_learning_factors, **kwargs):
		pass
	@abstractmethod
	def save(self, savefile):
		pass
	@abstractmethod
	def load(self, savefile):
		pass
	def parse_raw_df(raw_df, colnames = None):
		df = raw_df.drop(["date", "sector", "ticker", "record_id"], 1)
		raw_colnames = list(df)
		if colnames is not None:
			if len(raw_colnames) != len(colnames):
				raise Exception("No. of columns for prediction [%d] does not match with training data [%d]."%(len(raw_colnames), len(colnames)))
		else:
			colnames = raw_colnames
		X = df.as_matrix(colnames)
		dates = raw_df[['date']]
		return (X, dates, colnames)
	def prediction_to_df(dates, predictions):
		data = {'date': dates, 'label': predictions}
		result = pandas.DataFrame(data, index = dates, columns = ['date', 'label'])
		return result

class SimpleSVMModel(GenericMLModel):
	def __init__(self, **kwargs):
		self.model = SVC(kwargs)
		self.trained = False
		self.colnames = None
	def train(self, machine_learning_factors, labels, **kwargs):
		if self.trained:
			raise Exception("Model already trained.")
		X, _, self.colnames = GenericMLModel.parse_raw_df(machine_learning_factors)
		self.model.fit(X, labels)
		self.trained = True
	def predict(self, machine_learning_factors, **kwargs):
		if not self.trained:
			raise Exception("Model not trained.")
		X, dates, self.colnames = GenericMLModel.parse_raw_df(machine_learning_factors, self.colnames)
		predictions = self.model.predict(X)
		return prediction_to_df(dates, predictions)
	def save(self, savefile):
		if not self.trained:
			raise Exception("Model not trained.")
		joblib.dump(self.model, savefile)
	def load(self, savefile):
		if self.trained:
			raise Exception("Model already trained.")
		self.model = joblib.load(savefile)
		self.trained = True