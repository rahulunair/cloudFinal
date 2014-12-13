'''No sql pipeline to take text data in and to give api output'''

from couchdbkit import Server
import warnings
import json
import re
from FuelTruck import FuelTruck

warnings.filterwarnings("ignore")
# server object
#server = Server()
ft = FuelTruck('energy_data_01')
#data_match = re.compile('Net Generation : .*?: .*?: All Sectors : Annual', re.IGNORECASE)
data_match = re.compile('electric', re.IGNORECASE)


# create database
#db = server.get_or_create_db("energy_data")
count = 0
'''
with open("/home/rahul/programming/python/cloud_final_project/energy_analysis/data/ELEC.txt") as fh:
	
	print "Streaming data into nosql DB"
	for line in fh:
		count = count + 1
		line = json.loads(line)
		ft.save_dataset(line)
		#db.save_doc(line)
	print "Number of records saved: ", count
	
'''
print ft.get_data()	
for line in ft.get_data():
	#print "hello"
	#	print line.value['name']
	if data_match.search(line.value['name']):
		print 'Matching records: %s\n' %line.value['name']
	print "next line", count
	count +=1			

