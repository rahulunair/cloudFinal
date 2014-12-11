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
reg_pattern = re.compile('Net Generation : .*?: .*?: All Sectors : Annual',re.IGNORECASE)
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

#regex for extracting data for US state only
reg_us_state = re.compile('Net Generation : .*? : (.*?) : All Sectors : Annual',re.IGNORECASE)

#filtering out US state list only
filtered_data = []
for i in dataset:
    state_name = i['name']
    if reg_us_state.findall(state_name)[0] in state_names:
        filtered_data.append(i)

filtered_state_lst = []
for i in filtered_data:
    filtered_state_lst.append(i['name'])
#print filtered_state_lst

#regex for extracting distinct descrtions
reg_dis_name = re.compile('Net Generation : (.*?) : .*? : All Sectors : Annual',re.IGNORECASE)

#set up dictionary to store distinct descrtions
distnct_des = defaultdict(str)

for i in filtered_data:
    name = i['name']
    unique_name = reg_dis_name.findall(i['name'])[0]
    distnct_des[unique_name] = i['description']

#print distnct_des

#Create dictionary for all the sourcewise electricity data

#regex for statewise and sourcewise data grouping
state_match = re.compile('Net Generation : .*? : (.*?) : All Sectors : Annual',re.IGNORECASE)
source_match = re.compile('Net Generation : (.*?) : .*? : All Sectors : Annual',re.IGNORECASE)

dict_total_elec = defaultdict(dict)

#loop through filtered_data to populate dict_total_elec
for i in filtered_data:
    dict_year = {}
    dict_source = {}
    data = i['data']
    name = i['name']
    state = state_match.findall(name)[0]
    source = source_match.findall(name)[0]
    #changing source to all lowercase, placing _ instead of spaces, and removing parentheses
    source = source.replace(" ","_").replace("(","").replace(")","").lower()
    for pair in data:
        dict_year[pair[0]] = pair[1]
    dict_total_elec[state][source] = dict_year

states = []
sources = []

for state, source_dict in dict_total_elec.iteritems():
    states.append(state)
    sources.append(pd.DataFrame.from_dict(source_dict))
    
source_frame = pd.concat(sources, keys=states)

#name the indices
source_frame.index.names = ['state','year']
#replacing Nan by 0        
source_frame.fillna(0, inplace=True)
print (source_frame.head())

source_frame = source_frame.drop(['other_renewables_total','all_fuels'],1)
''' values for other_renewables_total,all_fuels have been removed as they are giving incorrect answers found out by 
sourcewise_yearly_total = source_frame.groupby(level='year').apply(sum)
sourcewise_yearly_total.apply(sum, axis=1) - 2*(sourcewise_yearly_total.all_fuels) - (sourcewise_yearly_total.other_renewables_total)'''

sourcewise_yearly_total = source_frame.groupby(level='year').apply(sum)

fig, axes = plt.subplots(nrows=4, ncols=4)
for i,series_name in enumerate(sourcewise_yearly_total.columns):
    row = int(floor(i/4))
    column = i % 4
    sourcewise_yearly_total[series_name].plot(figsize = (12,12),ax=axes[row,column]); axes[row,column].set_title(series_name)

plt.show()

#making hydro-electric_pumped_storage positive
source_frame['hydro-electric_pumped_storage'] = -1*source_frame['hydro-electric_pumped_storage']
source_frame.columns

#grouping of different form of energies
renewable_energy = source_frame[['conventional_hydroelectric','hydro-electric_pumped_storage','geothermal','other_biomass', 'wood_and_wood-derived_fuels','solar','wind']].apply(sum,axis=1)
natural_gas = source_frame['natural_gas']
coal_energy = source_frame['coal']
nuclear_energy = source_frame['nuclear']
other_energy = source_frame[['other','other_gases','petroleum_coke','petroleum_liquids']].apply(sum,axis=1)

#concatenating all the groups together
group_of_sources = pd.concat([renewable_energy, natural_gas, coal_energy, nuclear_energy, other_energy], axis=1)
group_of_sources.columns = ['renewable_energy', 'natural_gas','coal_energy','nuclear_energy','other_energy']
group_of_sources['all_sources'] = group_of_sources.renewable_energy + group_of_sources.natural_gas + group_of_sources.coal_energy + group_of_sources.nuclear_energy + group_of_sources.other_energy

group_of_sources.head()
