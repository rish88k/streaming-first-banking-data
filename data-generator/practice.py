"""import psycopg2
import random
import datetime
from psycopg2.extensions import cursor
from psycopg2.extras import execute_values
import faker


fake= faker();

customers= []

for _ in customers(10):
    customers.append((
        fake.firstname(),
        fake.lastname(),
        fake.email(),
        fake.phone(),
        fake.address(),
    ));


db_config= {
    "hostname": "localhost",
    "port": "5432",
    "database": "banking_db",
    "user": "admin",
    "role": "Admin"
}

def get_Connection():
    return psycopg2.connect(**db_config);

def insert_values():
    conn = get_Connection();
    cur= cursor.conn();
"""
"""
    SQL_QUERY= f
            INSERT INTO CUSTOMERS
            VALUES ( {customers.})
    

    conn.execute_values(SQL_QUERY, c)
    cur.fetchall()
"""