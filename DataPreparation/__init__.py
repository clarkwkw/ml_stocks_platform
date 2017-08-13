from TrainingDataPreparation import TrainingDataPreparation as TrainDP
from TestingDataPreparation  import TestingDataPreparation as TestDP
from ValidationDataPreparation import ValidationDataPreparation as ValidDP
from DataPreprocessing import DataPreprocessing as DP
from timeit import default_timer as timer
import multiprocessing

runtime = 0
runtime_lock = multiprocessing.Lock()
def TrainingDataPreparation(*args, **kwargs):
	global runtime
	start = timer()
	result = TrainDP(*args, **kwargs)
	end = timer()
	runtime_lock.acquire()
	runtime += end - start
	runtime_lock.release()
	return result

def TestingDataPreparation(*args, **kwargs):
	global runtime
	start = timer()
	result = TestDP(*args, **kwargs)
	end = timer()
	runtime_lock.acquire()
	runtime += end - start
	runtime_lock.release()
	return result

def ValidationDataPreparation(*args, **kwargs):
	global runtime
	start = timer()
	result = ValidDP(*args, **kwargs)
	end = timer()
	runtime_lock.acquire()
	runtime += end - start
	runtime_lock.release()
	return result

def DataPreprocessing(*args, **kwargs):
	global runtime
	start = timer()
	result = DP(*args, **kwargs)
	end = timer()
	runtime_lock.acquire()
	runtime += end - start
	runtime_lock.release()
	return result

__all__ = [TrainingDataPreparation, TestingDataPreparation, ValidationDataPreparation, DataPreprocessing]