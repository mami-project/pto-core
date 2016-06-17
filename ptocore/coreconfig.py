from pymongo import MongoClient
import json

class CoreConfig:
    def __init__(self, program_name: str, fp):
        assert(program_name in ['sensor', 'supervisor', 'validator', 'admin'])
        doc = json.load(fp)

        self.mongo = MongoClient(doc[program_name]['mongo_uri'])

        self.environment = doc['environment']

        # derive database and collection names
        ptocore_db_name = self.environment + "-core"
        temporary_db_name = self.environment + "-temp"
        observations_db_name = self.environment + "-obs"
        metadata_db_name, metadata_coll_name = doc['metadata_coll']

        # get databases
        self.ptocore_db = self.mongo[ptocore_db_name]
        self.temporary_db = self.mongo[temporary_db_name]
        self.observations_db = self.mongo[observations_db_name]

        # get collections
        self.analyzers_coll = self.ptocore_db.analyzers
        self.action_log = self.ptocore_db.action_log
        self.observations_coll = self.observations_db.observations
        self.idfactory_coll = self.ptocore_db.idfactory

        # get metadata collection
        self.metadata_db = self.mongo[metadata_db_name]
        self.metadata_coll = self.metadata_db[metadata_coll_name]

        # supervisor specific
        if program_name == "supervisor":
            self.supervisor_port = doc['supervisor']['listen_port']

        # admin specific
        if program_name == "admin":
            self.admin_host = doc['admin'].get('listen_host', 'localhost')
            self.admin_port = doc['admin'].get('listen_port', 5000)
            self.admin_static_path = doc['admin']['static_path']