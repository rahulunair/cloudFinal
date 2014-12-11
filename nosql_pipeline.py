'''No sql pipeline to take text data in and to give api output'''

from couchdbkit import Server
import warnings
import json
warnings.filterwarnings("ignore")
# server object
server = Server()

# create database
db = server.get_or_create_db("energy_data")
count = 0
with open("/home/rahul/programming/python/cloud_final_project/energy_analysis/data/ELEC.txt") as fh:
	for line in fh:
		count = count + 1
		line = json.loads(line)
		db.save_doc(line)
		print "record count: ", count
		