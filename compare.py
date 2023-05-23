from cassandra.cluster import Cluster
from pymongo import MongoClient
import mysql.connector
import happybase
import time
import sys

if len(sys.argv) != 2:
    print('Usage: python compare.py <num_rows>')
    sys.exit(1)

num_rows = int(sys.argv[1])

# Cassandra
print('Connecting to Cassandra database')
cluster = Cluster(['cassandra'])
session = cluster.connect()
session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1}")
session.set_keyspace('test')
session.execute('CREATE TABLE IF NOT EXISTS users (id int PRIMARY KEY, name text)')

# MongoDB
print('Connecting to MongoDB database')
client = MongoClient('mongo', 27017)
db = client['test']
users = db['users']

# MySQL
# Connect to MySQL server
cnx = mysql.connector.connect(user='root', password='password', host='mysql')

# Create database if it does not exist
cursor = cnx.cursor()
cursor.execute("CREATE DATABASE IF NOT EXISTS test")
cursor.close()

print('Connecting to MySQL database')
cnx = mysql.connector.connect(user='root', password='password',
                              host='mysql',
                              database='test')
cursor = cnx.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS users (id int PRIMARY KEY, name varchar(255))')

# HBase
print('Connecting to HBase database')
connection = happybase.Connection('hbase', 9090)
connection.open()
connection.create_table('users', {'info': {}})

# Insert data
insert_times = []
print(f'Performing write test on {num_rows} rows in each database')
start_time = time.time()

for i in range(num_rows):
    session.execute("INSERT INTO users (id, name) VALUES (%s, %s)", (i, f"user_{i}"))

insert_times.append(time.time() - start_time)

print('1/4')

start_time = time.time()

for i in range(num_rows):
    users.insert_one({'id': i, 'name': f'user_{i}'})

insert_times.append(time.time() - start_time)
print('2/4')

start_time = time.time()

for i in range(num_rows):
    cursor.execute("INSERT INTO users (id, name) VALUES (%s, %s)", (i, f"user_{i}"))
    cnx.commit()

insert_times.append(time.time() - start_time)
print('3/4')

start_time = time.time()

for i in range(num_rows):
    connection.table('users').put(f'user_{i}', {'info:name': f'user_{i}'})

insert_times.append(time.time() - start_time)
print('4/4')

#Read test
read_times = []
print(f'Performing read test on {num_rows} rows in each database')
start_time = time.time()

for i in range(num_rows):
    row = session.execute("SELECT name FROM users WHERE id = %s", (i,)).one()

read_times.append(time.time() - start_time)
print('1/4')

start_time = time.time()

for i in range(num_rows):
    result = users.find_one({'id': i})

read_times.append(time.time() - start_time)
print('2/4')

start_time = time.time()

for i in range(num_rows):
    cursor.execute("SELECT name FROM users WHERE id = %s", (i,))
    name = cursor.fetchone()[0]

read_times.append(time.time() - start_time)
print('3/4')

start_time = time.time()

for i in range(num_rows):
    row = connection.table('users').row(f'user_{i}')

read_times.append(time.time() - start_time)
print('4/4')


# Update test
rw_times = []
print(f'Performing update test on {num_rows} rows in each database')

start_time = time.time()

for i in range(num_rows):
        # Update the user's name
        name = f"user_{i}_updated"
        session.execute("UPDATE users SET name = %s WHERE id = %s", (name, i))

rw_times.append(time.time() - start_time)
print('1/4')

start_time = time.time()

for i in range(num_rows):
        # Update the user's name
        name = f"user_{i}_updated"
        users.update_one({'id': i}, {'$set': {'name': name}})

rw_times.append(time.time() - start_time)
print('2/4')

start_time = time.time()

for i in range(num_rows):
        # Update the user's name
        name = f"user_{i}_updated"
        cursor.execute("UPDATE users SET name = %s WHERE id = %s", (name, i))
        cnx.commit()

rw_times.append(time.time() - start_time)
print('3/4')

start_time = time.time()

for i in range(num_rows):
        # Update the user's name
        name = f"user_{i}_updated"
        connection.table('users').put(f'user_{i}', {'info:name': name})

rw_times.append(time.time() - start_time)
print('4/4')


# Clean up
print('Cleaning up databases')
session.execute('DROP TABLE IF EXISTS users')
session.execute('DROP KEYSPACE IF EXISTS test')
cluster.shutdown()

db.drop_collection('users')
client.close()

cursor.execute('DROP TABLE IF EXISTS users')
cnx.close()

connection.disable_table('users')
connection.delete_table('users')

# Print results
print('Comparison completed')
print(f'Num of rows: {num_rows}')
print('-' * 80)
print(f"{'Database':<10} {'Write Time':<15} {'Read Time':<15} {'Update Time':<15}")
print('-' * 80)
print(f"{'Cassandra':<10} {insert_times[0]:<15.4f} {read_times[0]:<15.4f} {rw_times[0]:<15.4f}")
print(f"{'MongoDB':<10} {insert_times[1]:<15.4f} {read_times[1]:<15.4f} {rw_times[1]:<15.4f}")
print(f"{'MySQL':<10} {insert_times[2]:<15.4f} {read_times[2]:<15.4f} {rw_times[2]:<15.4f}")
print(f"{'HBase':<10} {insert_times[3]:<15.4f} {read_times[3]:<15.4f} {rw_times[3]:<15.4f}")