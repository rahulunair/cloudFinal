import pymongo as pm


class MongoConnect:
	'''
	A simple class to connect to mongo DB
	'''
	def __init__(self):
		try:
			self.con = pm.MongoClient()
			print 'Connected to Mongo Db: ', self.con
			self.db = self.con.energy_data
			print 'connected to database: ',self.db
		except pm.errors.ConnectionFailure, e:
			print ' Cannot connect to database', e

	def conn(self):
		return self.db

	def close_con(self):
		self.con.close()			