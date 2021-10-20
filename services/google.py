
import json
from dotenv import load_dotenv
import os
import sys
from utils.filesystem import writers
from utils.common import setup_logging

# Google Packages
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2

# Getting environment variables
load_dotenv()

# Default Google Service variables
GOOGLE_AUTH_TYPE = os.environ["GOOGLE_AUTH_TYPE"]
GOOGLE_AUTH_URI = os.environ["GOOGLE_AUTH_URI"]
GOOGLE_TOKEN_URI = os.environ["GOOGLE_TOKEN_URI"]
GOOGLE_AUTH_PROVIDER_CERT_URL = os.environ["GOOGLE_AUTH_PROVIDER_CERT_URL"]
GOOGLE_PROJECT_ID = os.environ["GOOGLE_PROJECT_ID"]
GOOGLE_PRIVATE_KEY_ID = os.environ["GOOGLE_PRIVATE_KEY_ID"]
GOOGLE_PRIVATE_KEY = os.environ["GOOGLE_PRIVATE_KEY"]
GOOGLE_CLIENT_EMAIL = os.environ["GOOGLE_CLIENT_EMAIL"]
GOOGLE_CLIENT_ID = os.environ["GOOGLE_CLIENT_ID"]
GOOGLE_CERT_URL = os.environ["GOOGLE_CERT_URL"]


LOG = setup_logging(__name__)


class bigquery_service:
    def __init__(self, project_id=GOOGLE_PROJECT_ID, private_key_id=GOOGLE_PRIVATE_KEY_ID, private_key=GOOGLE_PRIVATE_KEY,
                client_email=GOOGLE_CLIENT_EMAIL, client_id=GOOGLE_CLIENT_ID, cert_url=GOOGLE_CERT_URL, verbose=True):
        
        self.project_id= project_id
        self.private_key_id = private_key_id
        self.private_key = private_key
        self.client_email = client_email
        self.client_id = client_id
        self.cert_url = cert_url
        self.verbose = verbose
        self.service_file = None


    def __enter__(self, *args):
        LOG.info("Creating Bigquery service") 
        self.authenticate()
        return self

    
    def __exit__(self, *args):
        LOG.info("Closing Bigquery service")   
        self.client.close()


    def authenticate(self):
        service_json = {
            "type": GOOGLE_AUTH_TYPE,
            "project_id": self.project_id,
            "private_key_id": self.private_key_id,
            "private_key": self.private_key,
            "client_email": self.client_email,
            "client_id": self.client_id,
            "auth_uri": GOOGLE_AUTH_URI,
            "token_uri": GOOGLE_TOKEN_URI,
            "auth_provider_x509_cert_url": GOOGLE_AUTH_PROVIDER_CERT_URL,
            "client_x509_cert_url": self.cert_url
            }

        self.service_file = writers.dict_to_file(service_json, "google_service_file.json", verbose=self.verbose)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = self.service_file
        self.client = bigquery.Client()
        LOG.info(f"Successfully authenticated to project {self.project_id}")


    def execute_sql(self, query, verbose=True):
        if verbose:
            LOG.info(f"Executing query\n{query}")
        return self.client.query(query).result()


    def return_query_dict(self, query):
        pass


    def return_query_df(self, query):
        pass
        #TODO not here yet


    def insert_df_records(self, df, schema, table):
        table_id = f"{self.project_id}.{schema}.{table}"
        self.client.load_table_from_dataframe(df, table_id)
        LOG.info(f"Upserted {df.size} records to {table_id}")


    def create_schema(self, name, replace=False):
        self.client.create_dataset(name, replace)
        LOG.info(f"Successfully Created Dataset {name}")


    def list_datasets(self):
        return self.client.list_datasets(self.project_id)


    def list_tables(self, dataset_id=None):
        dataset_ids = []
        if dataset_id:
            dataset_ids.append(dataset_id)
        else:
            for dataset_id in self.list_datasets():
                dataset_ids.append(dataset_id)

        tables = []
        for dataset_id in dataset_ids:
            dataset_tables = self.client.list_tables(dataset_id)
            tables.extend(dataset_tables)

        return tables
