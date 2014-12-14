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
#regex for gathering required da
db_conn=sqlite3.connect('final_proj1.db');#creating the db connection
cur=db_conn.cursor();# assigning the cursor to the db connection
state_names = [state.name for state in states.STATES]

# script for creating tables
table_creation='''drop table states;
drop table year;
drop table source_type;
drop table state_production;
drop table IMP_EXP;
drop table state_imp_exp;

CREATE TABLE states( ID integer(10) not null,
State_Name text(100) not null,
CONSTRAINT states_pk PRIMARY KEY (ID)
);

CREATE TABLE year( ID integer(10) not null,
year date not null,
CONSTRAINT year_pk PRIMARY KEY (ID)
);


CREATE TABLE source_type( ID integer(10) not null,
Source_Name text(100) not null,
Source_Description text(100) not null,
CONSTRAINT source_pk PRIMARY KEY (ID)
);

CREATE TABLE state_production( ID integer(10) not null,
Value real(100) not null,
source_id integer(10) not null,
state_id integer(10) not null,
year_id integer(10) not null,
CONSTRAINT state_prod_pk PRIMARY KEY (ID),
CONSTRAINT state_fk
    FOREIGN KEY (state_id)
    REFERENCES states(state_id),
CONSTRAINT source_fk
    FOREIGN KEY (source_id)
    REFERENCES source_type(source_id),
CONSTRAINT year_fk
    FOREIGN KEY (year_id)
    REFERENCES source_type(year_id))    ;

CREATE TABLE IMP_EXP( ID integer(10) not null,
Type text(10) not null,
CONSTRAINT IMP_EXP_pk PRIMARY KEY (ID)
);    

CREATE TABLE state_imp_exp( ID integer(10) not null,
Value real(100) not null,
type_id integer(10) not null,
state_id integer(10) not null,
year_id integer(10) not null,
CONSTRAINT state_imp_exp_pk PRIMARY KEY (ID),
CONSTRAINT state_fk FOREIGN KEY (state_id) REFERENCES states(state_id),
CONSTRAINT year_fk FOREIGN KEY (year_id) REFERENCES states(year_id),
CONSTRAINT type_fk FOREIGN KEY (type_id) REFERENCES IMP_EXP(type_id));'''



        




#regex for gathering electricity production data
reg_elec_prod = re.compile('Net Generation : .*?: .*?: All Sectors : Annual',re.IGNORECASE)
elec_prod_dataset = list()
     
with open(e_data) as fh:
	for r in fh:
		line = json.loads(r)
		if reg_elec_prod.search(line['name']):
			elec_prod_dataset.append(line)

#regex for extracting electricity production data for US state only
reg_us_state_elec_prod = re.compile('Net Generation : .*? : (.*?) : All Sectors : Annual',re.IGNORECASE)
elec_prod_filtered_dataset = list()
elec_prod_filtered_dataset_name=list()

#filtering out data for US state  only

for i in elec_prod_dataset:
    state_name = i['name']
    if reg_us_state_elec_prod.findall(state_name)[0] in state_names:
        elec_prod_filtered_dataset_name.append(i['name'])    
        elec_prod_filtered_dataset.append(i)

#print elec_prod_filtered_dataset
#print elec_prod_filtered_dataset_name

#regex for extracting distinct source_type
reg_dis_source_type = re.compile('Net Generation : (.*?) : .*? : All Sectors : Annual',re.IGNORECASE)

#set up dictionary to store distinct source_type
distnct_source_type = defaultdict(str)

for i in elec_prod_filtered_dataset:
    unique_name = reg_dis_source_type.findall(i['name'])[0]
    distnct_source_type[unique_name] = i['description']

distnct_source_type = {key:value.split(';')[0] for key, value in distnct_source_type.items()}

#set up dictionary to store distinct years
distnct_year = defaultdict()

for i in elec_prod_filtered_dataset:
    data=i['data']
    for x in data:
        unique_name = x[0]
        distnct_year[unique_name] = unique_name

#Create dictionary for all the sourcewise electricity production data

dict_total_elec = defaultdict(dict)

#loop through filtered_data to populate dict_total_elec
for i in elec_prod_filtered_dataset:
    dict_year = {}
    dict_source = {}
    data = i['data']
    name = i['name']
    state = reg_us_state_elec_prod.findall(name)[0]
    source = reg_dis_source_type.findall(name)[0]
    #changing source to all lowercase, placing _ instead of spaces, and removing parentheses
    source = source.replace(" ","_").replace("(","").replace(")","").lower()
    for pair in data:
        dict_year[pair[0]] = pair[1]
    dict_total_elec[state][source] = dict_year


source_type_id=None
year_val=None
year_id=None

###############################

def get_ie(location):
    data_match = re.compile('Electricity .*? the United States, .*?',re.IGNORECASE)
    units_match = re.compile('Billion Btu',re.IGNORECASE)
    raw_data = []
    with open(location) as myfile:
        for row in myfile:
            line = json.loads(row)
            if (data_match.search(line['name'])) and (units_match.search(line['units'])):
                raw_data.append(line)
        return raw_data
raw_data = get_ie(s_data)
print len(raw_data)


def filter_data(raw_data):
    the_range = len(raw_data)
    for number in range(the_range):
        int_numb = int(number)
        filtered_data = [x for x in raw_data[int_numb]['data'] if int(x[0]) > 2000]
        raw_data[int_numb]['data'] = filtered_data
    return raw_data

filtered_data = filter_data(raw_data)

def get_list_of_names(json_data):
    names = []
    for x in json_data:
        names.append(x['name'])
    return names

names = get_list_of_names(filtered_data)
print len(names)
print names[0]
#set up regular expressions to match the state and source
state_match = re.compile('Net Generation : .*? : (.*?) : All Sectors : Annual')
source_match = re.compile('Net Generation : (.*?) : .*? : All Sectors : Annual')

#create dictionary to store results

total_dict = defaultdict(dict)

#loop through filtered_data to populate data_dict
def make_dict(json_data):
    for x in json_data:
        year_dict = {}
        source_dict = {}
        #The list of lists
        data = x['data']
        # The name
        name = x['name']
        state = name[45:]
        source = name[:18]
        #change the source to all lowercase,  _ instead of spaces, and no parentheses
        source = source.lower()
        for pair in data:
            year_dict[pair[0]] = pair[1]
        total_dict[state][source] = year_dict
    return total_dict


total_dict = make_dict(filtered_data)
print total_dict['Texas']

states = []
sources = []

for state, source_dict in total_dict.iteritems():
    states.append(state)
    sources.append(pd.DataFrame.from_dict(source_dict))
    
import_export_frame = pd.concat(sources, keys=states)
import_export_frame.fillna(0, inplace=True)
import_export_frame.index.names = ['state','year']
import_export_btus = import_export_frame
import_export_btus.columns = [column.replace(" ","_") for column in import_export_btus.columns]
print import_export_btus.head()

import_export = import_export_btus * 0.29307107

import_export['export_minus_import'] = import_export.electricity_export - import_export.electricity_import
print import_export.head()




try:
        cur.executescript(table_creation);
        print ('tables created')
        for i in range(len(state_names)):
                cur.execute("insert into states values (?,?)", (i,state_names[i]));
        db_conn.commit();
        cur.execute("select *  from states")
        state_lst=cur.fetchall()
        #print state_lst
        i=0
        for x in distnct_source_type.keys():
                cur.execute("insert into source_type values (?,?,?)", (i,x,distnct_source_type[x]));
                i=i+1
        cur.execute("select *  from source_type")
        src_lst=cur.fetchall()
        #print src_lst
        i=0
        for x in distnct_year.keys():
                cur.execute("insert into year values (?,?)", (i,x));
                i=i+1
        cur.execute("select *  from year")
        yr_lst=cur.fetchall()
        #print yr_lst
        i=0
        for c in state_names:
                state_name=(c,)
                for r in cur.execute("select id from states where State_Name=?",state_name):
                        state_id=r[0]
                        for x in dict_total_elec[state_name[0]]:
                                source_name=(x,)
                                for a in cur.execute("select id from Source_type where Source_Name =?", source_name):
                                    source_type_id=a[0]
                                if source_type_id:
                                    for z in dict_total_elec[state_name[0]][source_name[0]]:
                                        year_val=(z,)
                                        data_val=dict_total_elec[state_name[0]][source_name[0]][year_val[0]]
                                        if data_val:
                                                for year_id in cur.execute("select id from year where year=?",(year_val)):
                                                        cur.execute("insert into state_production values (?,?,?,?,?)", (i,data_val,source_type_id,state_id,year_id[0]));  
                                                        i=i+1
        cur.execute("select * from state_production")
        st_prd=cur.fetchall()
        #print st_prd
        cur.execute('''select states.State_Name,year.year,source_type.Source_Name,sp.value from states,source_type,year,state_production sp where sp.state_id=states.id and sp.year_id=year.id and sp.source_id=source_type.id
                     and states.State_Name="Mississippi"''')
        q=cur.fetchall()
        #print 'state,year,source_type,prod_val'
        #print q
        
except:
     print "db_error"
     raise
finally:
     db_conn.close()





'''states = []
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

#group_of_sources_frame.head()'''

'''sales_dataset = list()
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
#total_sales_frame.head()

######################################

def get_ie(location):
    data_match = re.compile('Electricity .*? the United States, .*?',re.IGNORECASE)
    units_match = re.compile('Billion Btu',re.IGNORECASE)
    raw_data = []
    with open(location) as myfile:
        for row in myfile:
            line = json.loads(row)
            if (data_match.search(line['name'])) and (units_match.search(line['units'])):
                raw_data.append(line)
        return raw_data
#s_location='Mississippi'
raw_data = get_ie(s_data)
print len(raw_data)


def filter_data(raw_data):
    the_range = len(raw_data)
    for number in range(the_range):
        int_numb = int(number)
        filtered_data = [x for x in raw_data[int_numb]['data'] if int(x[0]) > 2000]
        raw_data[int_numb]['data'] = filtered_data
    return raw_data

filtered_data = filter_data(raw_data)

def get_list_of_names(json_data):
    names = []
    for x in json_data:
        names.append(x['name'])
    return names

names = get_list_of_names(filtered_data)
print len(names)
print names[0]
#set up regular expressions to match the state and source
state_match = re.compile('Net Generation : .*? : (.*?) : All Sectors : Annual')
source_match = re.compile('Net Generation : (.*?) : .*? : All Sectors : Annual')

#create dictionary to store results

total_dict = defaultdict(dict)

#loop through filtered_data to populate data_dict
def make_dict(json_data):
    for x in json_data:
        year_dict = {}
        source_dict = {}
        #The list of lists
        data = x['data']
        # The name
        name = x['name']
        state = name[45:]
        source = name[:18]
        #change the source to all lowercase,  _ instead of spaces, and no parentheses
        source = source.lower()
        for pair in data:
            year_dict[pair[0]] = pair[1]
        total_dict[state][source] = year_dict
    return total_dict


total_dict = make_dict(filtered_data)
print total_dict['Texas']

states = []
sources = []

for state, source_dict in total_dict.iteritems():
    states.append(state)
    sources.append(pd.DataFrame.from_dict(source_dict))
    
import_export_frame = pd.concat(sources, keys=states)
import_export_frame.fillna(0, inplace=True)
import_export_frame.index.names = ['state','year']
import_export_btus = import_export_frame
import_export_btus.columns = [column.replace(" ","_") for column in import_export_btus.columns]
print import_export_btus.head()

import_export = import_export_btus * 0.29307107

import_export['export_minus_import'] = import_export.electricity_export - import_export.electricity_import
print import_export.head()


source_combine = DataFrame(group_of_sources_frame.copy())
source_combine.columns = ["production_" + column for column in group_of_sources_frame.columns]
sales_combine = DataFrame(total_sales_frame.copy())
sales_combine.columns = ["sales_" + column for column in total_sales_frame.columns]
#price_combine = DataFrame(price.copy())
#price_combine.columns = ["price_" + column for column in price_combine.columns]
#revenue_combine = DataFrame(revenue.copy())
#revenue_combine.columns = ["revenue_" + column.lower() for column in revenue_combine.columns]
all_data = pd.concat([source_combine,import_export,sales_combine], join="inner", axis=1)
all_data.head()

statewise_yearly_average = all_data.groupby(level=1).apply(np.mean)
print statewise_yearly_average.head()'''








'''state_inp=raw_input('Enter the state name : ')

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
    production_plot_by_state(state_inp)'''
    
 ########################################################################
 
 data_t = range(15)
data_y = [5,6,15,20,21,22,26,42,45,60,65,68,75,80,79]

def holt_alg(h, y_last, y_pred, T_pred, alpha, beta):
    pred_y_new = alpha * y_last + (1-alpha) * (y_pred + T_pred * h)
    pred_T_new = beta * (pred_y_new - y_pred)/h + (1-beta)*T_pred
    return (pred_y_new, pred_T_new)

def smoothing(t, y, alpha, beta):
    # initialization using the first two observations
    pred_y = y[1]
    pred_T = (y[1] - y[0])/(t[1]-t[0])
    y_hat = [y[0], y[1]]
    # next unit time point
    t.append(t[-1]+1)
    print t
    for i in range(2, len(t)):
        h = t[i] - t[i-1]
        pred_y, pred_T = holt_alg(h, y[i-1], pred_y, pred_T, alpha, beta)
        y_hat.append(pred_y)
    return y_hat

import matplotlib.pyplot as plt
plt.plot(data_t, data_y, 'x-')
plt.hold(True)

pred_y = smoothing(data_t, data_y, alpha=.9, beta=0.5)
plt.plot(data_t[:len(pred_y)], pred_y, 'rx-')
plt.show()


                




