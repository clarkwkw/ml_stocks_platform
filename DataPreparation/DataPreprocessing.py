import pandas as pd
import json

from .DataNormalization import DataNormalization

def DataPreprocessing(flag, stock_data, validate_data = None, preprocessing_file = None):
    if flag == 'train':
        stock_data, normalization_info = DataNormalization(stock_data)
        with open(preprocessing_file.split('_')[0] + '_normalization_info.json','w') as f:
            json.dump(normalization_info,f,indent=4)
        with open(preprocessing_file,'w') as f:
            json.dump({'normalization':preprocessing_file.split('_')[0] + '_normalization_info.json'},f,indent=4)
    elif flag == 'test':
        with open(preprocessing_file,'r') as f:
            preprocessing_info = json.load(f)
        with open(preprocessing_info['normalization'],'r') as f:
            normalization_info = json.load(f)
        stock_data = DataNormalization(stock_data, normalization_info = normalization_info)
    elif flag == 'validate':
        stock_data, validate_data = DataNormalization(stock_data, validate_data)    
        
    if validate_data is not None:
        return (stock_data, validate_data)
    
    return stock_data