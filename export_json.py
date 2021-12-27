import json
import psycopg2

username = 'borysenko'
password = 'admin'
database = 'ddd'
host = 'localhost'
port = '5432'

TABLES = ['Player_ex_csv', 'Match_csgo_ex_csv', 'Team_ex_csv']

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

result = {}

with conn:
    cur = conn.cursor()
    for table in TABLES:
        cur.execute('SELECT * FROM ' + table)
        rows = []
        for i in cur:
            rows.append(dict(zip([x[0] for x in cur.description], i)))
        result[table] = rows
with open('BorysenkoDB.json', 'w') as out:
    json.dump(result, out, default=str)