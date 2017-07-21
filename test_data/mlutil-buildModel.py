import pandas
import MachineLearningModelUtilities as MLUtils
import DataPreparation as DataPrep

model_savedir = "./test_output/"
def test():
    print("Loading data...")
    stock_data = pandas.read_csv('./test_data/10tickers_wo_sector_fill.csv')
    stock_data.drop('sector', axis = 1, inplace = True)
    stock_data.dropna(subset=['last_price'],inplace=True)
    stock_data.fillna(value = 0, inplace = True)

    dataset = DataPrep.ValidationDataPreparation(stock_data, False, 10, 15, 1, period = 1)
    train_data, valid_data = dataset[len(dataset)//2]

    print("Training SVM...")
    model = MLUtils.buildModel("SVM", "./test_output/preprocessing_file.json", train_data, False, 10, 15, 1)
    preprocessed_valid = DataPrep.DataPreprocessing(flag = "test", stock_data = valid_data, preprocessing_file = "./test_output/preprocessing_file.json")

    model.save(model_savedir)

    model = None
    model = MLUtils.loadTrainedModel(model_savedir)
    print("Evaluation long - 10")
    print(MLUtils.evaluateModel(model, preprocessed_valid, 10, "long"))

    print("Evaluation short - 10")
    print(MLUtils.evaluateModel(model, preprocessed_valid, 10, "short"))

    print("Evaluation long_short - 10")
    print(MLUtils.evaluateModel(model, preprocessed_valid, 10, "long_short"))


    print("Training NN...")
    model = MLUtils.buildModel("NN", "./test_output/preprocessing_file.json", train_data, False, 10, 15, 1)
    preprocessed_valid = DataPrep.DataPreprocessing(flag = "test", stock_data = valid_data, preprocessing_file = "./test_output/preprocessing_file.json")

    model.save(model_savedir)

    model = None
    model = MLUtils.loadTrainedModel(model_savedir)
    print("Evaluation long - 10")
    print(MLUtils.evaluateModel(model, preprocessed_valid, 10, "long"))

    print("Evaluation short - 10")
    print(MLUtils.evaluateModel(model, preprocessed_valid, 10, "short"))

    print("Evaluation long_short - 10")
    print(MLUtils.evaluateModel(model, preprocessed_valid, 10, "long_short"))