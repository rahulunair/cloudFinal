#==============
#mongodb
#db.eia_data.find("name": {$regex: "/Net Generation : .*?: .*?: All Sectors : Annual/i"})
#==============
import pymongo as pm

try:
	conn = pm.MongoClient()
	print "Connected to Mongo Db"
except pm.errors.ConnectionFailure, e:
	print " Cannot connect to database"

conn	


