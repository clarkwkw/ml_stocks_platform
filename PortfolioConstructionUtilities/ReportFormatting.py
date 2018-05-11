# coding: utf-8
import pandas
import numbers
import xlwt

def reformat_report(src_report, target_path, sectors = None, positions = None):
	if type(src_report) != pandas.DataFrame:
		src_report = pandas.read_csv(src_report)

	if sectors is None:
		sectors = src_report['sector'].unique()
		
	elif type(sectors) is not list and sectors.strip().lower() == "all":
		sectors = ["all"]

	if positions is None:
		positions = src_report['position'].unique()

	formatter = Report_Formatter(sectors, positions)
	for _, row in src_report.iterrows():
		sector = row['sector']
		position = row['position']
		sector_return = row['return']
		sector_std = row['std']
		formatter.fill_value(sector, position, "return", sector_return)
		formatter.fill_value(sector, position, "std", sector_std)
		ratio = float("nan")
		try:
			ratio = sector_return/sector_std
		except:
			pass
		formatter.fill_value(sector, position, "return/std", ratio)

	formatter.to_file(target_path)


class Report_Formatter:
	def __init__(self, sectors, positions, max_horizontal_sector = 3):
		self._sector_index = {}

		if type(sectors) is not list and sectors.strip().lower() == "all":
			sectors = ["all"]

		self.n_sector = len(sectors)
		for i in range(self.n_sector):
			self._sector_index[sectors[i]] = i

		self._position_index = {}
		self.n_position = len(positions)
		for i in range(self.n_position):
			self._position_index[positions[i]] = i

		self._max_horizontal_sector = max_horizontal_sector

		self.__init_matrix()
	'''
	Outfit of one block:
    <--------------  One Major Col --------------> ^
	| Energy | r      | σ      | r/σ    | (spare | |
	| Long   | 20.3%  | 13.3%  | 1.526  | col )  | One
	| Short  | -10.1% | 5.0%   | -2.02  |   .    | Major
	| Total  | 30.4%  | 10.3%  | 2.951  |   .    | Row
	|................(spare row).................| |
	<--------------  One Major Col --------------> v
	'''
	def __init_matrix(self):
		n_major_rows = self.n_sector//self._max_horizontal_sector + (self.n_sector % self._max_horizontal_sector > 0)
		self.major_row_size = 2 + self.n_position
		self.major_col_size = 5
		total_rows = (2 + self.n_position)*n_major_rows
		total_cols = 0
		if n_major_rows == 1:
			total_cols = 5*self.n_sector
		
		self.workbook = xlwt.Workbook(encoding='utf8')
		self.worksheet = self.workbook.add_sheet('result')
		bold_font = xlwt.Font()
		bold_font.bold = True
		title_style = xlwt.XFStyle()
		title_style.font = bold_font

		for sector, i in self._sector_index.items():
			major_row_index = i // self._max_horizontal_sector
			major_col_index = i % self._max_horizontal_sector
			self.worksheet.write(major_row_index * self.major_row_size, major_col_index * self.major_row_size, sector, title_style)
			self.worksheet.write(major_row_index * self.major_row_size, major_col_index * self.major_row_size + 1, "r", title_style)
			self.worksheet.write(major_row_index * self.major_row_size, major_col_index * self.major_row_size + 2, "σ", title_style)
			self.worksheet.write(major_row_index * self.major_row_size, major_col_index * self.major_row_size + 3, "r/σ", title_style)

			for position, j in self._position_index.items():
				self.worksheet.write(major_row_index * self.major_row_size + j + 1, major_col_index * self.major_row_size, position, title_style)

		self.content_style = xlwt.XFStyle()

	def fill_value(self, sector, position, data_type, value):
		major_row_index = self._sector_index[sector] // self._max_horizontal_sector
		major_col_index = self._sector_index[sector] % self._max_horizontal_sector
		minor_row_index = self._position_index[position] + 1
		minor_col_index = 0
		if data_type == "return":
			minor_col_index = 1
		elif data_type == "std":
			minor_col_index = 2
		elif data_type == "return/std":
			minor_col_index = 3
		else:
			raise Exception("Unexpected data_type '%s'"%data_type)
		self.worksheet.write(major_row_index * self.major_row_size + minor_row_index, major_col_index * self.major_col_size + minor_col_index, parse(value), self.content_style)

	def to_file(self, file):
		self.workbook.save(file)

def parse(val):
	if isinstance(val, numbers.Number):
		return "%.3f"%val
	else:
		return str(val)


