import psycopg2
import boto3
import json

sm = boto3.client('secretsmanager')

secret = sm.get_secret_value(SecretId='postgres-credentials')
secrets_dict = json.loads(secret['SecretString'])

def connect():
    ''' Instantiates a connection to the db'''
    conn = psycopg2.connect(
        host=secrets_dict['host'],
        database='postgres',
        user=secrets_dict['username'],
        password=secrets_dict['password'])
    return conn

def instantiate_db():
    conn = connect()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT * FROM covid19 LIMIT 1;')
        return
    except UndefinedTable:
        print('creating table')
        cursor.execute(
            '''CREATE TABLE covid19 (
                date TIMESTAMP PRIMARY KEY,
                cases NUMERIC,
                deaths NUMERIC,
                recovered NUMERIC
            );

            INSERT INTO covid19 (date, cases, deaths, recovered) VALUES ('2019-01-01', 0, 0, 0);'''
        )
        conn.commit()
        cursor.close()
        conn.close()
    
 

def get_latest_date():
    ''' Returns the current ltest record in the db'''
    conn = connect()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM covid19 ORDER BY date DESC LIMIT 1;')
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return results[0][0]
    

def load_to_db(row):
    ''' Insert new rows to the db'''
    conn = connect()
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO covid19 (date, cases, deaths, recovered) VALUES (%s, %s, %s, %s)', (row['Date'], row['cases'], row['deaths'], row['Recovered'])
        )
    conn.commit()
    cursor.close()
    conn.close()
    
