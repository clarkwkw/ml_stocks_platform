def min_max_scale(X, info, new_max=1.0, new_min=0.0):
    return (new_max-new_min)*(X-info['min'])/(info['max']-info['min']) + new_min

def standardization(X, info):
    return (X-info['mean'])/info['std']

normalization_method = {'min_max_scale':min_max_scale, 'standardization':standardization}