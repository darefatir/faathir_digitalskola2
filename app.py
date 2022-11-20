import os
import connection
import sqlparse
import pandas as pd

if __name__ == '__main__':
    print('[INFO] ETL service is starting...')
    # data source connection
    conf = connection.config('marketplace_prod')
    conn, engine = connection.psql_conn(conf, 'DataSource')
    cursor = conn.cursor()

    # connection DWH
    conf_dwh = connection.config('dwh')
    conn_dwh, engine_dwh = connection.psql_conn(conf_dwh, 'DataWarehouse')
    cursor_dwh = conn_dwh.cursor()

    # obtain query string
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(path_query+'query.sql', 'r').read(), strip_comments=True
    ).strip()

    # create DWH schema
    path_dwh_design = os.getcwd()+'/query/'
    dwh_design = sqlparse.format(
        open(path_dwh_design+'dwh_design.sql', 'r').read(), strip_comments=True
    ).strip()

    try:
        print('[INFO] ETL service is running')
        df = pd.read_sql(query, engine)
        # print(df)

        # create DWH schema
        cursor_dwh.execute(dwh_design)
        conn_dwh.commit()

        #ingest data to DWH
        df.to_sql('dim_orders', engine_dwh, if_exists='append', index=False)
        print('[INFO] ETL service is success!')

    except Exception as e:
        print('[INFO] ETL service is failed')