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
print "parent_directory:", data_files
e_data = data_files+"/ELEC.txt"
s_data = data_files+"/SEDS.txt"
#regex for gathering required data
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
#print (names)

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
#values for other_renewables_total,all_fuels have been removed as they are giving incorrect answers found out by 
 #sourcewise_yearly_total = source_frame.groupby(level='year').apply(sum)
#sourcewise_yearly_total.apply(sum, axis=1) - 2*(sourcewise_yearly_total.all_fuels) - (sourcewise_yearly_total.other_renewables_total)

sourcewise_yearly_total = source_frame.groupby(level='year').apply(sum)

fig, axes = plt.subplots(nrows=4, ncols=4)
for i,series_name in enumerate(sourcewise_yearly_total.columns):
    row = int(floor(i/4))
    column = i % 4
    sourcewise_yearly_total[series_name].plot(figsize = (12,12),ax=axes[row,column]); axes[row,column].set_title(series_name)

#plt.show()

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
group_of_sources_frame = pd.concat([renewable_energy, natural_gas, coal_energy, nuclear_energy, other_energy], axis=1)
group_of_sources_frame.columns = ['renewable_energy', 'natural_gas','coal_energy','nuclear_energy','other_energy']
group_of_sources_frame['all_sources'] = group_of_sources_frame.renewable_energy + group_of_sources_frame.natural_gas + group_of_sources_frame.coal_energy + group_of_sources_frame.nuclear_energy + group_of_sources_frame.other_energy

#group_of_sources_frame.head()

sales_dataset = list()
names = list()
sales_dict = defaultdict(str)
#regex for statewise sales data
reg_sales = re.compile('Retail Sales of Electricity : .*?: .*?: Annual',re.IGNORECASE)

with open(e_data) as fh:
    for r in fh: 
        line = json.loads(r)
        if reg_sales.search(line['name']) and "Revenue" not in line['name']:#Revenue need not to be included
            sales_dict[line['name']]=line['data']
            sales_dataset.append(line)

          
for i in sales_dataset:
    names.append(i['name'])
print names

df = pd.DataFrame(sales_dict)

#Let's pull the column header into each row/record
d2 = df.unstack().reset_index()

#And rename the column header to get the data
d2 = d2.rename(columns={0: "Sales"})

#Then split the column header field into its components
parts = d2.pop("level_0").str.split(":")
#parts.to_csv("parts")

#And now create new columns based on those components
d2["state"] = [p[1].strip() for p in parts]
d2["Sector"] = [p[2].strip() for p in parts]

#Let's create a dictionary we can use to switch the level_1 fields to the actual years
temp_vals = range(0,13)
temp_years = sorted(range(2001,2014), reverse=True)

year_setter_dict = {}
for i in range(len(temp_vals)):
   year_setter_dict[temp_vals[i]] = temp_years[i]

#Turns the a column into years based on the dictionary matching
d2['level_1'] = d2['level_1'].map(lambda x: year_setter_dict[x])

#Now let's get rid of the year inside of the cell
import operator
d2['year']=d2.Sales.apply(operator.itemgetter(-1), axis=1)
# d2['year']=d2['year'].astype(str)

d2['year'].dtype
d2.rename(columns={'year':'Data'}, inplace=True)

del d2['Sales']
d2.rename(columns={'Data': 'Sales'}, inplace=True)
d2.rename(columns={'level_1': 'year'}, inplace=True)

#Let's get rid of all the geographies that aren't states
# states_list = list(c_pop_df['NAME'])
d2 = d2[d2['state'].isin(state_names)]

#Set the indices to State and Year
d3 = d2.set_index(['state', 'year'])
d3.Sales = d3.Sales.astype(float)
#Then let's make columns that are for each of the sectors
d4 = d3.reset_index()
d4.dtypes
d4.year = d4.year.astype(str)

total_sales_frame = pivot_table(d4, values = 'Sales', rows = ['state','year'], columns = 'Sector')

total_sales_frame.columns = [column.lower().replace(" ","_") for column in total_sales_frame.columns]
total_sales_frame.fillna(0, inplace=True)
total_sales_frame.head()

source_combine = DataFrame(group_of_sources_frame.copy())
source_combine.columns = ["production_" + column for column in group_of_sources_frame.columns]
sales_combine = DataFrame(total_sales_frame.copy())
sales_combine.columns = ["sales_" + column for column in total_sales_frame.columns]
'''price_combine = DataFrame(price.copy())
price_combine.columns = ["price_" + column for column in price_combine.columns]
revenue_combine = DataFrame(revenue.copy())
revenue_combine.columns = ["revenue_" + column.lower() for column in revenue_combine.columns]'''
all_data = pd.concat([source_combine,sales_combine], join="inner", axis=1)
all_data.head()

statewise_yearly_average = all_data.groupby(level=1).apply(np.mean)
statewise_yearly_average.head()

state_inp=raw_input('Enter the state name : ')

def production_plot_by_state(state_nm):
    curve_col_name = ['renewable_energy','natural_gas','coal_energy','nuclear_energy','other_energy']
    curve_name_w_unit = ['renewables','natural gas','coal','nuclear','other']

    figu = figure(figsize=(8,8))
    ax1 = figu.add_subplot(111)

    column_df = DataFrame(all_data.loc[state_nm][curve_col_name])
    ax1.plot(column_df.index, column_df[curve_col_name[0]], label = curve_name_w_unit[0].split("_")[0])
    ax1.plot(column_df.index, column_df[curve_col_name[1]], label = curve_name_w_unit[1].split("_")[0])
    ax1.plot(column_df.index, column_df[curve_col_name[2]], label = curve_name_w_unit[2].split("_")[0])
    ax1.plot(column_df.index, column_df[curve_col_name[3]], label = curve_name_w_unit[3].split("_")[0])
    ax1.plot(column_df.index, column_df[curve_col_name[4]], label = curve_name_w_unit[4].split("_")[0])
    ax1.set_title(state_nm, fontsize=18)
    ax1.grid()
    ax1.set_ylabel("Production (gigawatt-hours)", fontsize=16)
    ax1.set_xlabel("Year", fontsize=16)
    ax1.legend(loc=0)



for state_inp in sorted(state_names):
    production_plot_by_state(state_inp)
    
    


                



