import MachineLearningModelUtilities as MLUtil
import numpy as np
import pandas

data = {
	'date': ['2016-12-01', '2016-12-02', '2016-12-03', '2016-12-04'], 
	'x1': [1, 1, 0, 0], 
	'x2': [1, 0, 1, 0], 
	'sector': ['Energy', 'Energy', 'Energy', 'Energy'],
	'ticker': ['A', 'A', 'A', 'A'],
	'record_id': [1, 2, 3, 4]
	}
labels = np.asarray([1, 0, 0, 1])
df = pandas.DataFrame(data)

m1 = MLUtil.SimpleSVMModel()
m1.train(df, labels)

m1.save('m1.save')

m2 = MLUtil.SimpleSVMModel()
m2.load('m1.save')

print(m2.predict(df))