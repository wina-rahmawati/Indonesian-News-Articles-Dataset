import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook


def get_data_postgres(query, conn_id, schema, dir_save):
    sql_stmt = query
    hook = PostgresHook(
        postgres_conn_id=conn_id,
        schema=schema
    )
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_stmt)
    df_temp = pd.DataFrame(cursor.fetchall())
    print(df_temp.head())
    print([ x[0] for x in cursor.description ])
    df_temp.columns = [ x[0] for x in cursor.description ]
    print(df_temp.head())
    df_temp.to_csv(dir_save, sep=';', index=False)
