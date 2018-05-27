#!/usr/bin/env python3

# Copyright [2018] [Joel Leagues email: Scourge @ protomail.com]

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
class csv_parser:

	def __init__(self,csv_file,line_start=0,line_end=0): 
		self._csv_file = csv_file
		self._csv_lines = open(csv_file,'r').read()
		if line_start == 0:
			self._line_start = line_start+1
		else:
			self._line_start = line_start

		if line_end == 0:
			blank_lines = len(re.findall(re.compile('^(\\s*?)$',re.M),self._csv_lines))
			self._line_end = len(self._csv_lines.split('\n'))-blank_lines-1
			# -1 is due to the column headers
		else:
			self._line_end = line_end

		self._headers = []
		self._data = [[]]

	def parse_csv_file(self):
		unsplit_csv = self._csv_lines.replace("'","\\'").split('\n') 
		# protect the good quotes and split the lines to make the 1st level array
		csv = [re.split(r"(?<=[^\\]),",x) for x in unsplit_csv]
		# split the comma seperated values into the arrays

		self._headers = csv[0]
		self._data = (csv[1::])
		return self

	def headers(self):
		return self._headers

	def columns(self): # alias for headers
		return self._headers

	def data(self):
		return self._data

	def values(self): # alias for data
		return self._data

	def rows(self):
		return self._data

	def yield_headers(self):
		for i in self._headers:
			yield i

	def yield_data(self):
		for i in self._data:
			yield i

	def csv_file(self):
		return self._csv_file
