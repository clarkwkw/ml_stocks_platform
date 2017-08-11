import abc, six
from utils import id_fields

@six.add_metaclass(abc.ABCMeta)
class GenericMLModel(object):

	@abc.abstractmethod
	def __init__(self, _factors = None, **kwargs):
		self._model = None
		self._trained = False
		self._factors = _factors

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
		id_frame = raw_df[id_fields]
		df = raw_df.drop(id_fields, 1)
		raw_factors = list(df)
		if self._factors is not None:
			if len(raw_factors) != len(self._factors):
				raise Exception("No. of columns to parse [%d] does not match with self._factors [%d]."%(len(raw_factors), len(self._factors)))
		else:
			self._factors = raw_factors
		parsed_matrix = df.as_matrix(self._factors)
		return (parsed_matrix, id_frame)

	def _prediction_to_df(self, id_frame, predictions):
		result = id_frame.copy()
		result['pred'] = predictions	
		return result