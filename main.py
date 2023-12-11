import pandas
import time
import sqlite3
import duckdb
from sqlalchemy import create_engine, text
import os
import psycopg2
import datetime
from sqlalchemy.orm import sessionmaker
import sqlalchemy

path_file = input("Введите путь до вашего csv файла: ")
dbname = input("Введите имя базы данных на вашем сервере PostgreSQL: ")
username = input("Введите username на вашем сервере PostgreSQL: ")
password = input("Введите password базы данных на вашем сервере PostgreSQL: ")
# for psycorg2 and duckDB
requests_type_1 = ["SELECT VendorID, count(*) FROM trips group by 1",
                   "SELECT passenger_count, avg(total_amount) FROM trips group by 1",
                   "SELECT passenger_count, EXTRACT(YEAR from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2",
                   """SELECT passenger_count, EXTRACT(YEAR from tpep_pickup_datetime), round(trip_distance), count(*) 
               FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc"""]
# for pandas, sqlite and sqlaclhemy
requests_type_2 = ["SELECT VendorID, count(*) FROM trips group by 1",
                   "SELECT passenger_count, avg(total_amount) FROM trips group by 1",
                   "SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) as p, count(*) FROM trips GROUP BY "
                   "1, 2",
                   """SELECT passenger_count, strftime('%Y', tpep_pickup_datetime) as p, round(trip_distance), count(*) 
               FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc"""]
columns = ["NUMBER INTEGER PRIMARY KEY",
           "VendorID INTEGER",
           "tpep_pickup_datetime DATETIME",
           "tpep_dropoff_datetime DATETIME",
           "passenger_count FLOAT",
           "trip_distance FLOAT",
           "RatecodeID FLOAT",
           "store_and_fwd_flag TEXT",
           "PULocationID INTEGER",
           "DOLocationID INTEGER",
           "payment_type INTEGER",
           "fare_amount FLOAT8",
           "extra FLOAT8",
           "mta_tax FLOAT8",
           "tip_amount FLOAT8",
           "tolls_amount FLOAT8",
           "improvement_surcharge FLOAT8",
           "total_amount FLOAT8",
           "congestion_surcharge FLOAT8",
           "airport_fee FLOAT8"]

results_test = []
results_ = [[] for i in range(4)]
count_tests = 11
################################################### -psycopg2
params = {"dbname": dbname,
          "user": username,
          "password": password,
          "host": "localhost",
          "port": "5432"}

csv_file = open(path_file)

connect_psycopg2 = psycopg2.connect(**params)
cursor = connect_psycopg2.cursor()
cursor.execute("DROP TABLE IF EXISTS trips")
query_for_create_table = f"CREATE TABLE trips ({', '.join(list(map(lambda x: x.replace('DATETIME', 'timestamp'), columns)))})"
cursor.execute(query_for_create_table)

cursor.copy_expert("COPY trips from STDIN WITH CSV HEADER", csv_file)

for i in range(4):
    for j in range(count_tests):
        start_time = time.perf_counter()
        cursor.execute(requests_type_1[i])
        end_time = time.perf_counter()
        results_test.append(end_time - start_time)
    results_test.sort()
    results_[i].append(results_test[count_tests // 2])
    results_test.clear()
cursor.close()
connect_psycopg2.close()
################################################### -SQLite
connection_tiny = sqlite3.connect(":memory:")
tiny = pandas.read_csv("data/tiny_data.csv", sep=",")
bd = tiny.to_sql('trips', connection_tiny, if_exists='replace', index=False)

cursor = connection_tiny.cursor()
cursor.execute("ALTER TABLE trips RENAME TO trips_old")
cursor.execute(f"CREATE TABLE trips ({', '.join(columns)})")
name_columns = [i.split()[0] for i in columns]
cursor.execute(f"INSERT INTO trips ({', '.join(name_columns)}) SELECT {', '.join(name_columns)} FROM trips_old")
cursor.execute(f"DROP TABLE trips_old")

for i in range(4):
    for j in range(10):
        start_time = time.perf_counter()
        cursor.execute(requests_type_2[i])
        end_time = time.perf_counter()
        results_test.append(end_time - start_time)
    results_test.sort()
    results_[i].append(results_test[count_tests // 2])
    results_test.clear()
cursor.close()
connection_tiny.close()
try:
    os.remove("memory:")
except:
    pass
################################################### duckdb
connect_duckdb = duckdb.connect()
connect_duckdb.sql(
    f"CREATE TABLE trips as SELECT * FROM read_csv_auto('{path_file}')")
for i in range(4):
    for j in range(10):
        start_time = time.perf_counter()
        connect_duckdb.execute(requests_type_1[i])
        end_time = time.perf_counter()
        results_test.append(end_time - start_time)
    results_test.sort()
    results_[i].append(results_test[count_tests // 2])
    results_test.clear()
connect_duckdb.close()
################################################### pandas
engine_pandas = create_engine("sqlite:///memory:")
df_pandas = pandas.read_csv(path_file, sep=",")
df_pandas.to_sql("trips", engine_pandas, index=False)
df_pandas['tpep_pickup_datetime'] = pandas.to_datetime(df_pandas['tpep_pickup_datetime'])
df_pandas['tpep_dropoff_datetime'] = pandas.to_datetime(df_pandas['tpep_dropoff_datetime'])
connect_pandas = engine_pandas.connect()
for i in range(4):
    for j in range(10):
        sql_request = sqlalchemy.text(requests_type_2[i])
        start_time = time.perf_counter()
        pandas.read_sql_query(sql_request, connect_pandas)
        end_time = time.perf_counter()
        results_test.append(end_time - start_time)
    results_test.sort()
    results_[i].append(results_test[count_tests // 2])
    results_test.clear()
connect_pandas.close()
try:
    os.remove("memory:")
except:
    pass
################################################### SQLAlchemy
converter_types = {
    "INTEGER": sqlalchemy.Integer,
    "TEXT": sqlalchemy.String,
    "FLOAT": sqlalchemy.Float,
    "DATETIME": sqlalchemy.DateTime,
    "FLOAT8": sqlalchemy.Float,
}
converter_python_types = {
    "INTEGER": int,
    "TEXT": str,
    "FLOAT": float,
    "REAL": float,
    "DATETIME": datetime.datetime.strptime,
    "FLOAT8": float,
}

engine_sqlachemy = sqlalchemy.create_engine("sqlite:///memory:")
connect_sqlaclhemy = engine_sqlachemy.connect()
session = sessionmaker(bind=engine_sqlachemy)()

metadata_obj = sqlalchemy.MetaData()
columns_create = []
for col in columns:
    col_split = col.split()
    columns_create.append(
        sqlalchemy.Column(col_split[0], converter_types[col_split[1]],
                          primary_key=True if len(col_split) > 2 else False))
trips = sqlalchemy.Table(
    "trips",
    metadata_obj,
    *columns_create
)
metadata_obj.create_all(engine_sqlachemy)

csv_data = pandas.read_csv(path_file)
csv_data = csv_data.values.tolist()
for i in csv_data:
    ins = trips.insert()
    d = {}
    for count, j in enumerate(columns):
        if j.split()[1] == "DATETIME":
            d[j.split()[0]] = converter_python_types[j.split()[1]](i[count], r"%Y-%m-%d %H:%M:%S")
        else:
            d[j.split()[0]] = converter_python_types[j.split()[1]](i[count])
    session.execute(ins, d)

for i in range(4):
    for j in range(10):
        sql_request = sqlalchemy.text(requests_type_2[i])
        start_time = time.perf_counter()
        session.execute(sql_request).fetchall()
        end_time = time.perf_counter()
        results_test.append(end_time - start_time)
    results_test.sort()
    results_[i].append(results_test[count_tests // 2])
    results_test.clear()
connect_sqlaclhemy.close()
try:
    os.remove("memory:")
except:
    pass
# print(results_)
name_columns_answer_table = ['query/module', 'psycopg2', 'SQLite', 'DuckDB', 'pandas', 'SQLalchemy']
for c in name_columns_answer_table:
    print('%-25s' % c, end='')
print()
for row in range(4):
    print('%-25s' % f'Query_{row+1}', end='')
    for v in results_[row]:
        print('%-25s' % round(v, 3), end='')
    print()
