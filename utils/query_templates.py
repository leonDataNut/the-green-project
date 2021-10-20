MERGE_UPDATE_STATEMENT = """
MERGE INTO {target_schema}.{target_table} as trg
USING {source_schema}.{source_table} as src

ON {join_keys}

WHEN MATCHED THEN UPDATE SET {update_keys}

WHEN NOT MATCHED THEN INSERT ({source_insert_columns}) VALUES({target_insert_columns})
"""


MERGE_UPDATE_FROM_QUERY_STATEMENT = """
MERGE INTO {target_schema}.{target_table} as trg
USING ({query}) as src

ON {join_keys}

WHEN MATCHED THEN UPDATE SET {update_keys}

WHEN NOT MATCHED THEN INSERT ({source_insert_columns}) VALUES({target_insert_columns})
"""



DELETE_BY_MATCH_STATEMENT = """
MERGE INTO {target_schema}.{target_table} as trg
USING {source_schema}.{source_table} as src

ON {join_keys}
WHEN MATCHED THEN DELETE
"""


LIST_TABLE_COLUMNS_STATEMENT = """
SELECT * FROM `{schema}.INFORMATION_SCHEMA.COLUMNS`
where table_name = '{table}'
"""