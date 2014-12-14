import pymongo as pm
'''
Before using this class please import all data sources into the mongodb using the command:

"mongoimport --db <db-name> --collection <coll-name> --type json --file seed.json --jsonArray"

If mongodb has been configured correctly, then it should take a second to write
about 1 million rows in fire and forget mode. After which using views from mongo_pipeline can
access data in the file.

Be sure to close the database once the use is complete, not in between, a connection pool
is available to mongo client which will take care of connections during the course of the program.

Presently, the db is connected to localhost at 27017. As this is  a nosql database, which can be distributed
accross nodes, it would be seemless to extend the functionality.
'''


class MongoConnect:
	'''
	A simple class to connect to mongo DB
	'''
	def __init__(self):
		try:
			self.con = pm.MongoClient()
			print 'Connected to Mongo Db: ', self.con
		except pm.errors.ConnectionFailure, e:
			print ' Cannot connect to database', e

	def energy_data (self):
		'''
		energy databasereturn a table handle
		'''
		self.db = self.con.energy_data
		print 'connected to database: ',self.db
		return self.db

	def seds_table (self):
		'''
		seds databasereturns a table handle
		'''
		self.db = self.con.seds_data
		print 'connected to seds database: ', self.db
		return self.db


	def close_con(self):
		'''
		close connection to the database
		'''
		self.con.close()