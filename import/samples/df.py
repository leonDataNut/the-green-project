import pandas as pd
from services.google import bigquery_service
from utils.query_helpers import upsert_from_df
from utils.common import STAGING_SCHEMA

# A Sample dataset that loads data from a pandas dataframe to a bigquery table
data = [{"car":"Honda","color":"red"},{"car":"Toyota","color":"green"}]
df = pd.DataFrame(data)

# Calling the built sql helper function to upsert the dataframe to the import table in a standard way
with bigquery_service() as service:
    service.execute_sql(f"CREATE OR REPLACE TABLE {STAGING_SCHEMA}.testing(Car STRING, Color STRING)")
    upsert_from_df(service,STAGING_SCHEMA,"testing",df,["car"])