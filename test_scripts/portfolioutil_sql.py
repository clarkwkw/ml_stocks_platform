from PortfolioConstructionUtilities import DownloadTableFileFromMySQL

def test():
	print("Downloading data..")
	DownloadTableFileFromMySQL("US", ["Energy", "Materials"], factors = ["last_price"], start_date = "2010-01-01", output_dir = "./test_output")