import os
import collections
import re
from MongoConnect import MongoConnect
#constants

field   = 'name'
field_1 = 'units' 

e_query_1 = 'Net Generation : .*?: .*?: All Sectors : Annual'
e_query_2 = 'Net Generation : (.*?) : .*? : All Sectors : Annual'

s_query_1 = 'Electricity .*? the United States, .*?'  # data
s_query_2 =  'Billion Btu' #units

# global variables

q1_list = list()
q2_list = list()
uniq_dict = collections.defaultdict(str)
mongo = MongoConnect()


# connecting to energy_database 
e_mongo = mongo.energy_data() # includes eia and seds table

# connecting to seds database
#s_mongo = mongo.seds_data()



def test_list(q_list):
	for i in q_list:
		print i, '\n'


def find_row(field, query):  # eia_data
	'''
	A view to return all rows in the dataset whose 'name' field matches the regex 'query'
	'''
	# creating views
	raw_data = []
	for row in e_mongo.eia_data.find({field: {'$regex':query, '$options':'$i'}}, {'_id' : 0}):
		raw_data.append(row)
	return raw_data

def find_name (field, query):  # eia_data
	'''
	A view to return all names in the dataset that match the regex query
	'''
	names = []
	for name in e_mongo.eia_data.find({field: {'$regex':query, '$options':'$i'}}, {'_id' : 0, field : 1}):
		names.append(name['name'])
	return names	

def find_uniq_descriptions (field, query):  # eia_data
	'''
	A view to list all energy sources with their descriptions
	'''
	uniq_data = collections.defaultdict(str)
	for r in find_row(field, query):
		if r['name'].split(':')[1] not in uniq_data:
			uniq_data[r['name'].split(':')[1]] = r['description']
	return uniq_data



def get_ie(field,field_1, query, query_1):  #seds data	
	'''
	A view to get complete data based on name and units
	'''
	raw_data = []
	for row in e_mongo.eia_data.find({field: {'$regex':query, '$options':'$i'}}, {'_id' : 0}) and \
	e_mongo.eia_data.find({field_1: {'$regex':query_1, '$options':'$i'}}, {'_id' : 0}):
		raw_data.append(row)
	return raw_data



# testing modules 

q1_list = find_row(field, e_query_1)
q2_list = find_name(field, e_query_1)
e_types = find_uniq_descriptions(field, e_query_2)
s_list= get_ie(field, field_1, s_query_1, s_query_2)


#test_list(q1_list)
#test_list(q2_list)
test_list(s_list)

 # closing db
mongo.close_con()

