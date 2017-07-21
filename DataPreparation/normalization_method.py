import numpy as np

def min_max_scale(X, info, new_max=1.0, new_min=0.0):
	if info['max'] == info['min']:
		v = np.array([0.5 for _ in range(len(X))])
	else:
		v = (new_max-new_min)*(X-info['min'])/(info['max']-info['min']) + new_min

	return v

def standardization(X, info):
    return (X-info['mean'])/info['std']

normalization_method = {'min_max_scale':min_max_scale, 'standardization':standardization}