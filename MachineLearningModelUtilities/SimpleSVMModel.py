import config
import json
from GenericMLModel import GenericMLModel
from utils import *
from sklearn.svm import SVR
from sklearn.externals import joblib

class SimpleSVMModel(GenericMLModel):
	def __init__(self, _factors = None, **kwargs):
		super(self.__class__, self).__init__(_factors)
		if "max_iter" not in kwargs:
			self._model = SVR(max_iter = config.svm_max_iter, **kwargs)
		else:
			self._model = SVR(**kwargs)

	def train(self, machine_learning_factors, labels, **kwargs):
		if self._trained:
			raise Exception("Model already trained.")
		parsed_matrix, _ = self._parse_raw_df(machine_learning_factors)
		self._model.fit(parsed_matrix, labels, **kwargs)
		self._trained = True

	def predict(self, machine_learning_factors, **kwargs):
		if not self._trained:
			raise Exception("Model not trained.")
		parsed_matrix, id_frame = self._parse_raw_df(machine_learning_factors)
		predictions = self._model.predict(parsed_matrix, **kwargs)
		return prediction_to_df(id_frame, predictions)

	def save(self, savedir):
		if not self._trained:
			raise Exception("Model not trained.")
		joblib.dump(self._model, savedir+'/svmmodel.pkl')
		json_dict = {	'model_type': 'SVM', 
						'init_paras': {	'_factors': self._factors}
					}
		with open(savedir+'/model.conf', "w") as f:
			json.dump(json_dict, f)

	@staticmethod
	def load(savedir):
		with open(savedir+"/model.conf", "r") as f:
			json_dict = json.load(f)
		model = SimpleSVMModel(**json_dict["init_paras"])
		model._model = joblib.load(savedir+"/svmmodel.pkl")
		model._trained = True
		return model