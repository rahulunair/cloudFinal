import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import codecs
import os
import re
import csv
import os
#import census
from us import states
from collections import defaultdict
from pandas import *
from math import floor
from itertools import islice
from pylab import figure, show, savefig
from pylab import *
from pandas import DataFrame, Series
from matplotlib.lines import *
from matplotlib.colors import NoNorm

#for regression
import statsmodels.api as sm


# global variables

data_files = os.path.join(os.pardir, "data")
e_data = data_files+"/ELEC.txt"
s_data = data_files+"/SEDS.txt"
reg_pattern = re.compile('Net Generation : .*?: .*?: All Sectors : Annual')
dataset = list()
names = list()



state_names = [state.name for state in states.STATES]
#print state_names

with open(e_data) as fh:
	for r in fh:
		line = json.loads(r)
		if reg_pattern.search(line['name']):
			dataset.append(line)
for i in dataset:
	names.append(i['name'])
print (names)	