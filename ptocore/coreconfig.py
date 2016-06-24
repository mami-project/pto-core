import json

from pymongo import MongoClient
import dpath.util


class CoreConfig:
    def __init__(self, program_name: str, fps):
        assert(program_name in ['sensor', 'supervisor', 'validator', 'admin'])
        doc = {}
        for fp in fps:
            doc_load = json.load(fp)
            dpath.util.merge(doc, doc_load)

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
            self.supervisor_spark = doc['supervisor']['spark']
            self.supervisor_distributed = doc['supervisor']['distributed']
            self.supervisor_ensure_clean_repo = doc['supervisor']['ensure_clean_repo']

        # admin specific
        if program_name == "admin":
            self.admin_static_path = doc['admin']['static_path']
            self.admin_base_repo_path = doc['admin']['base_repo_path']
            self.admin_temp_repo_path = doc['admin']['temp_repo_path']
