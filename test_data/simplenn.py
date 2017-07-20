import sys, os
import numpy as np
import pandas
import random
import MachineLearningModelUtilities as MLUtil

def __maths_func(x1, x2, x3):
	return x1 - x2 +x3

def __classify(y):
	if y > 0:
		return 1
	else:
		return 0

def __new_point(data, labels):
	data['date'].append('2016-01-01')
	data['ticker'].append('ABC US Equity')
	data['record_id'].append(1)
	x1 = random.uniform(-1000, 1000)
	x2 = random.uniform(-1000, 1000)
	x3 = random.uniform(-1000, 1000)
	y = __classify(__maths_func(x1, x2, x3))
	data['x1'].append(x1)
	data['x2'].append(x2)
	data['x3'].append(x3)
	labels.append(y)
	return data, labels

def __generate_data(n = 500):
	data = {
		'date': [], 
		'x1': [], 
		'x2': [], 
		'x3': [],
		'ticker': [],
		'record_id': []
		}
	labels = []
	for i in range(n):
		data, labels = __new_point(data, labels) 
	labels = np.asarray(labels)
	data = pandas.DataFrame(data)
	zeros = np.sum(labels == 0)
	print('Distribution: 0: %d, 1: %d'%(zeros, n - zeros))
	return data, labels

def accuracy(predict_df, labels):
	return np.mean((predict_df["pred"] > 0.5) == labels)

def test():
	train_data, train_label = __generate_data(n = 500)
	test_data, test_label = __generate_data(n = 100)

	m1 = MLUtil.SimpleNNModel(_factors = ['x1', 'x2', 'x3'])
	m1.train(train_data, train_label)
	prediction = m1.predict(test_data)
	print("M1 Accuracy: %.4f"%accuracy(prediction, test_label))

	m2 = MLUtil.SimpleNNModel(_factors = ['x1', 'x2', 'x3'])
	m2.train(train_data, train_label, adapative = False, max_iter = 100)
	prediction = m2.predict(test_data)
	print("M2 Accuracy: %.4f"%accuracy(prediction, test_label))

	prediction = m1.predict(test_data)
	print("M1 Accuracy: %.4f"%accuracy(prediction, test_label))

	m1.save('./test_output')
	m3 = MLUtil.SimpleNNModel.load('./test_output')
	prediction = m3.predict(test_data)
	print("M3 Accuracy: %.4f"%accuracy(prediction, test_label))

	prediction = m2.predict(test_data)
	print("M2 Accuracy: %.4f"%accuracy(prediction, test_label))

	prediction = m1.predict(test_data)
	print("M1 Accuracy: %.4f"%accuracy(prediction, test_label))
