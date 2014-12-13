import os
import collections
import re
from MongoConnect import MongoConnect
#constants

field = 'name'
query_1 = 'Net Generation : .*?: .*?: All Sectors : Annual'
query_2 = 'Net Generation : (.*?) : .*? : All Sectors : Annual'

# global variables

q1_list = list()
q2_list = list()
uniq_dict = collections.defaultdict(str)
mongo = MongoConnect()


# connecting to database
mc = mongo.conn() 


def test_list(q_list):
	for i in q_list:
		print i, '\n'


def find_row(field, query): 
	'''
	A view to return all rows in the dataset whose 'name' field matches the regex 'query'
	'''
	# creating views
	raw_data = []
	for row in mc.eia_data.find({field: {'$regex':query, '$options':'$i'}}, {'_id' : 0}):
		raw_data.append(row)
	return raw_data

def find_name (field, query):
	'''
	A view to return all names in the dataset that match the regex query
	'''
	names = []
	for name in mc.eia_data.find({field: {'$regex':query, '$options':'$i'}}, {'_id' : 0, field : 1}):
		names.append(name['name'])
	return names	

def find_uniq_descriptions (field, query):
	'''
	A view to list all energy sources with their descriptions
	'''
	uniq_data = collections.defaultdict(str)
	for r in find_row(field, query):
		if r['name'].split(':')[1] not in uniq_data:
			uniq_data[r['name'].split(':')[1]] = r['description']
	return uniq_data


# testing modules

q1_list = find_row(field, query_1)
q2_list = find_name(field, query_1)
e_types = find_uniq_descriptions(field, query_2)


#test_list(q1_list)
test_list(q2_list)

 # closing db
mongo.close_con()

