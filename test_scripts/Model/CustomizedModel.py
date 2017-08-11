from MachineLearningModelUtilities import GenericMLModel
from sklearn.naive_bayes import MultinomialNB
from sklearn.externals import joblib
import json

metaparas_set = [{"alpha": 1.5}, {"alpha": 2.5}]
class Model(GenericMLModel):
	def __init__(self, _factors, **kwargs):
		super(self.__class__, self).__init__(_factors)
		self._model = MultinomialNB(**kwargs)

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
		return self._prediction_to_df(id_frame, predictions)

	def save(self, savedir):
		if not self._trained:
			raise Exception("Model not trained.")
		joblib.dump(self._model, savedir+'/MNBmodel.pkl')
		json_dict = {	'model_type': 'Custom', 
						'init_paras': {	'_factors': self._factors}
					}
		with open(savedir+'/model.conf', "w") as f:
			json.dump(json_dict, f)

	@staticmethod
	def load(savedir):
		with open(savedir+"/model.conf", "r") as f:
			json_dict = json.load(f)
		model = Model(**json_dict["init_paras"])
		model._model = joblib.load(savedir+"/MNBmodel.pkl")
		model._trained = True
		return model
