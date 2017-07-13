import sys, os
import numpy as np
import pandas
import random
sys.path.insert(1, os.path.join(sys.path[0], '..'))
import MachineLearningModelUtilities as MLUtil

def maths_func(x1, x2, x3):
	return x1 - x2 +x3

def classify(y):
	if y > 0:
		return 1
	else:
		return 0

def new_point(data, labels):
	data['date'].append('2016-01-01')
	data['sector'].append('Energy')
	data['ticker'].append('ABC US Equity')
	data['record_id'].append(1)
	x1 = random.uniform(-1000, 1000)
	x2 = random.uniform(-1000, 1000)
	x3 = random.uniform(-1000, 1000)
	y = classify(maths_func(x1, x2, x3))
	data['x1'].append(x1)
	data['x2'].append(x2)
	data['x3'].append(x3)
	labels.append(y)
	return data, labels

def generate_data(n = 500):
	data = {
		'date': [], 
		'x1': [], 
		'x2': [], 
		'x3': [],
		'sector': [],
		'ticker': [],
		'record_id': []
		}
	labels = []
	for i in range(n):
		data, labels = new_point(data, labels) 
	labels = np.asarray(labels)
	data = pandas.DataFrame(data)
	zeros = np.sum(labels == 0)
	print('Distribution: 0: %d, 1: %d'%(zeros, n - zeros))
	return data, labels

train_data, train_label = generate_data(n = 500)
test_data, test_label = generate_data(n = 100)

m1 = MLUtil.SimpleNNModel(_factors = ['x1', 'x2', 'x3'])
m1.train(train_data, train_label)
prediction = m1.predict(test_data)
print("M1 Accuracy: %.4f"%(np.mean(test_label == prediction['label'])))

m2 = MLUtil.SimpleNNModel(_factors = ['x1', 'x2', 'x3'])
m2.train(train_data, train_label, adapative = False, max_iter = 100)
prediction = m2.predict(test_data)
print("M2 Accuracy: %.4f"%(np.mean(test_label == prediction['label'])))

prediction = m1.predict(test_data)
print("M1 Accuracy: %.4f"%(np.mean(test_label == prediction['label'])))

m1.save('./test_output')
m3 = MLUtil.SimpleNNModel.load('./test_output')
prediction = m3.predict(test_data)
print("M3 Accuracy: %.4f"%(np.mean(test_label == prediction['label'])))

prediction = m2.predict(test_data)
print("M2 Accuracy: %.4f"%(np.mean(test_label == prediction['label'])))

prediction = m1.predict(test_data)
print("M1 Accuracy: %.4f"%(np.mean(test_label == prediction['label'])))