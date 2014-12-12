import couchdb
import couchdb.design



COUCH_SERVER = 'http://127.0.0.1:5984/'


class FuelTruck(object):
    '''
    database class to save, retrieve and count the number of datasets. Go to http://localhost:5984/_utils/index.html 
    for a front end view of the db
    '''

    def __init__(self, dbname, url=COUCH_SERVER):
        try:
            self.server = couchdb.Server(url=url)
            self.db = self.server.create(dbname)
            self._create_views()
        except couchdb.http.PreconditionFailed:
            self.db = self.server[dbname]

    def _create_views(self):
        count_map = 'function(doc) { emit(doc.id, 1); }'
        count_reduce = 'function(keys, values) { return sum(values); }'
        view = couchdb.design.ViewDefinition('energy_data', 'count_records', count_map, reduce_fun=count_reduce)
        view.sync(self.db)

        get_data = 'function(doc) { emit(("0000000000000000000"+doc.id).slice(-19), doc); }'
        view = couchdb.design.ViewDefinition('energy_data', 'get_data', get_data)
        view.sync(self.db)

    def save_dataset(self, line):
        line['id'] = line['name']
        self.db.save(line)

    def count_records(self):
        for doc in self.db.view('energy_data/count_records'):
            return doc.value

    def get_data(self):
        return self.db.view('energy_data/get_data')

    def delete_data(self):
        self.db.delete()		
