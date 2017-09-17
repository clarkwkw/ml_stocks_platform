import os
import pandas
import DataPreparation as DataPrep

def test():
	print("Loading data...")
	
	energy_data = pandas.read_csv('./test_data/Energy-10.csv')
	energy_data.dropna(subset=['last_price'],inplace=True)
	energy_data.fillna(value = 0, inplace = True)

	materials_data = pandas.read_csv('./test_data/Materials-10.csv')
	materials_data.dropna(subset=['last_price'],inplace=True)
	materials_data.fillna(value = 0, inplace = True)

	os.chdir("./test_output")

	print("Generating labels for Energy..")
	energy_data = DataPrep.TargetLabelGeneration(energy_data, B_top = 15, B_bottom = 15, target_label_holding_period = 1)
	energy_data.to_csv("Energy.csv", index = False)

	print("Generating labels for Materials..")
	materials_data = DataPrep.TargetLabelGeneration(materials_data, B_top = 15, B_bottom = 15, target_label_holding_period = 1)
	materials_data.to_csv("Materials.csv", index = False)

	prep_runtime = DataPrep.runtime
	print(">> Preparation Module: {:02.0f}:{:02.0f}:{:05.2f}".format(prep_runtime//3600, (prep_runtime%3600)//60, prep_runtime%60))
