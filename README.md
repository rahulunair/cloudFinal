Cloud Final Project
==========

Final project for cloud and big data analytics class


With this application, we are trying to model big data analysis with a nosql front end, an analysis core and a sql backend
to store the results. This work is mainly done using MongoDB, python modules and sqlite.

Before using this app, please import all data sources(goto : http://www.eia.gov/beta/api/bulkfiles.cfm and download SEDS.txt, ELEC.txt) into the mongodb using the command:

"mongoimport --db <db-name> --collection <coll-name> --type json --file seed.json --jsonArray"

If mongodb has been configured correctly, then it should take a few seconds to write
about 1 million rows in fire and forget mode. After which using views from mongo_pipeline can
access data in the file.

Be sure to close the database once the use is complete, not in between, a connection pool
is available to mongo client which will take care of connections during the course of the program.

Presently, the db is connected to localhost at 27017. As this is  a nosql database, which can be distributed
accross nodes, it would be seemless to extend the functionality.
