from utils.query_templates import MERGE_UPDATE_STATEMENT, DELETE_BY_MATCH_STATEMENT, LIST_TABLE_COLUMNS_STATEMENT, MERGE_UPDATE_FROM_QUERY_STATEMENT
from utils.common import setup_logging, IMPORT_SCHEMA, STAGING_SCHEMA, REPORTING_SCHEMA

LOG = setup_logging(__name__)

LOG_COLUMNS = {
    "src_loaded_at": "Timestamp", 
    "src_modified_at": "Timestamp"
    }

PANDAS_DTYPES_BQ_MAP = {
    "int64": "INT64",
    "float64": "DECIMAL",
    "bool": "BOOL",
    "datetime64": "DATETIME"
}


def add_log_columns(service, schema, table, verbose=True):
    columns_sql = LIST_TABLE_COLUMNS_STATEMENT.format(schema=schema, table=table)
    columns = [ row.get("column_name","").lower() for row in service.execute_sql(columns_sql, False)]
    
    for column, col_type in LOG_COLUMNS.items():
        if column.lower() not in columns:
            sql = f"ALTER TABLE {schema}.{table} ADD COLUMN {column} {col_type}"
            service.execute_sql(sql, verbose)
        

######################################
# Upsert Helpers
######################################

def upsert_from_table(service, src_join_columns, src_ordered_columns,
                src_schema, src_table, trg_schema, trg_table, trg_join_columns=None, 
                trg_ordered_columns=None, verbose=True, distinct_keys=True):

    trg_join_columns = trg_join_columns if trg_join_columns else src_join_columns
    if not trg_ordered_columns:
        trg_ordered_columns = []
        trg_ordered_columns.extend(src_ordered_columns)

    for log_col in LOG_COLUMNS.keys():
        src_ordered_columns.append(log_col)
        trg_ordered_columns.append(log_col)
    src_sql = (f"SELECT *, CURRENT_TIMESTAMP() as src_loaded_at, CURRENT_TIMESTAMP() as src_modified_at FROM {src_schema}.{src_table}")

    join_keys =  ",".join([f"trg.{x}=src.{trg_join_columns[i]}" for i,x in enumerate(src_join_columns)])
    update_keys = ",".join([f"trg.{x}=src.{trg_ordered_columns[i]}" for i,x in enumerate(src_ordered_columns)])
    source_insert_columns = ",".join(src_ordered_columns) 
    target_insert_columns = ",".join(trg_ordered_columns)


    if not distinct_keys:
        delete_sql = DELETE_BY_MATCH_STATEMENT.format(
            target_schema = trg_schema,
            target_table = trg_table,
            source_schema = src_schema,
            source_table = src_table,
            join_keys=join_keys, 
            update_keys=update_keys, 
            source_insert_columns=source_insert_columns,
            target_insert_columns=target_insert_columns
            )

        service.execute_sql(delete_sql, verbose)

    sql = MERGE_UPDATE_FROM_QUERY_STATEMENT.format(
        target_schema = trg_schema,
        target_table = trg_table,
        query = src_sql,
        join_keys=join_keys, 
        update_keys=update_keys, 
        source_insert_columns=source_insert_columns,
        target_insert_columns=target_insert_columns
        )

    add_log_columns(service, trg_schema, trg_table, verbose)
    service.execute_sql(sql, verbose)


def upsert_from_df(service, trg_schema, trg_table, df, src_join_columns,
                trg_join_columns=None, trg_ordered_columns=None, verbose=True, distinct_keys=True):

    src_ordered_columns = [x for x in df.columns]
    trg_join_columns = trg_join_columns if trg_join_columns else src_join_columns
    trg_ordered_columns = trg_ordered_columns if trg_ordered_columns else src_ordered_columns

    join_keys =  ",".join([f"trg.{x}=src.{trg_join_columns[i]}" for i,x in enumerate(src_join_columns)])
    update_keys = ",".join([f"trg.{x}=src.{trg_ordered_columns[i]}" for i,x in enumerate(src_ordered_columns)])
    source_insert_columns = ",".join(src_ordered_columns)
    target_insert_columns = ",".join(trg_ordered_columns)

    columns = df.columns
    columns_declare = ",\n".join([f"{x} {PANDAS_DTYPES_BQ_MAP.get(df[x].dtypes,'STRING')}" for x in columns])
    src_table = trg_table

    create_import_sql = f"""CREATE TABLE IF NOT EXISTS {IMPORT_SCHEMA}.{src_table}
        (
        {columns_declare}
        )
    """
        
    trg_delete_sql = DELETE_BY_MATCH_STATEMENT.format(
        target_schema = trg_schema,
        target_table = trg_table,
        source_schema = IMPORT_SCHEMA,
        source_table = src_table,
        join_keys=join_keys, 
        update_keys=update_keys, 
        source_insert_columns=source_insert_columns,
        target_insert_columns=target_insert_columns
        )


    trg_update_sql = MERGE_UPDATE_STATEMENT.format(
        target_schema = trg_schema,
        target_table = trg_table,
        source_schema = IMPORT_SCHEMA,
        source_table = src_table,
        join_keys=join_keys, 
        update_keys=update_keys, 
        source_insert_columns=source_insert_columns,
        target_insert_columns=target_insert_columns
        )


    # Creating the import table if not exists
    service.execute_sql(create_import_sql, verbose)

    # Inserting records from the df to import table
    service.execute_sql(f"TRUNCATE TABLE {IMPORT_SCHEMA}.{src_table}")
    service.insert_df_records(df, IMPORT_SCHEMA, src_table)

    # Deleting all matching records first if no distinct key
    if not distinct_keys:
        service.execute_sql(trg_delete_sql, verbose)
    
    # Upserting records from the import table
    service.execute_sql(trg_update_sql, verbose)


    upsert_from_table(service, src_join_columns, src_ordered_columns, IMPORT_SCHEMA, src_table, 
                trg_schema, trg_table, trg_join_columns=None, trg_ordered_columns=None, verbose=True, distinct_keys=True)


import pandas as pd
from services.google import bigquery_service
data = [{"car":"Honda","color":"red"},{"car":"Toyota","color":"green"}]
df = pd.DataFrame(data)

with bigquery_service() as service:
    #service.execute_sql(f"CREATE OR REPLACE TABLE {STAGING_SCHEMA}.testing(Car STRING, Color STRING)")
    upsert_from_df(service,STAGING_SCHEMA,"testing",df,["car"])