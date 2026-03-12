#import needed libraries
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pyodbc
import pandas as pd
import os

#get password from environmnet var
pwd = os.environ['PGPASS']
uid = os.environ['PGUID']
#sql db details
driver = "{SQL Server Native Client 11.0}"
server = "localhost"
database = "SalesDB"

#extract data from sql server
def extract():
    src_conn = None
    try:
        src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '\\SQLEXPRESS01' + ';DATABASE=' + database + ';Trusted_Connection=yes')
        print("✅ Koneksi SQL Server berhasil")  # tambah ini
        src_cursor = src_conn.cursor()
        src_cursor.execute(""" SELECT s.name + '.' + t.name AS table_name
                                FROM sys.tables t
                                JOIN sys.schemas s ON t.schema_id = s.schema_id
                                WHERE t.name IN ('Product','ProductSubcategory','ProductCategory','SalesTerritory','FactInternetSales') """)
        src_tables = src_cursor.fetchall()
        print("Tables ditemukan:", src_tables)  # tambah ini
        for tbl in src_tables:
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        if src_conn:
            src_conn.close()

#load data to postgres
def load(df, tbl):
    try:
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/SalesDB')
        print(f'Importing {len(df)} rows to table stg_{tbl}...')
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        print(f"Data imported successful for table {tbl}")
    except Exception as e:
        print("Data load error: " + str(e))
try:
    extract()
except Exception as e:
    print("ETL process error: " + str(e))