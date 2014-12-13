#==============
#mongodb
#db.eia_data.find("name": {$regex: "/Net Generation : .*?: .*?: All Sectors : Annual/i"})
#==============
import pymongo as pm
import os
import collections

#constants

field = 'name'
query_1 = 'Net Generation : .*?: .*?: All Sectors : Annual'
query_2 = 'Net Generation : (.*?) : .*? : All Sectors : Annual'

# global variables

q1_list = list()
q2_list = list()
uniq_dict = collections.defaultdict(str)

try:
	conn = pm.MongoClient()
	print 'Connected to Mongo Db: ', conn
	db = conn.energy_data
	print 'connected to database: ',db

except pm.errors.ConnectionFailure, e:
	print ' Cannot connect to database', e

def test_list(q_list):
	for i in q_list:
		print i, '\n'


def find_row(field, query): 
	'''
	A view to return all rows in the dataset whose 'name' field matches the regex 'query'
	'''
	# creating views
	raw_data = []
	for row in db.eia_data.find({field: {'$regex':query, '$options':'$i'}}, {'_id' : 0}):
		raw_data.append(row)
	return raw_data

def find_name (field, query):
	'''
	A view to return all names in the dataset that match the regex query
	'''
	names = []
	for name in db.eia_data.find({field: {'$regex':query, '$options':'$i'}}, {'_id' : 0, field : 1}):
		names.append(name['name'])
	return names	

def find_uniq_descriptions (field, query):
	uniq_data = collections.defaultdict(str)
	for r in find_row(field, query):
		name = r['name']
		uniq_data[name] = r
	return uniq_data





# testing modules

#q1_list = find_row(field, query)
#q2_list = find_name(field, query)
q3_list =  find_uniq_descriptions(field, query_2)


#test_list(q1_list)
#test_list(q2_list)
for k, v in q3_list.items():
	if not k:
		print k
		key -= k


