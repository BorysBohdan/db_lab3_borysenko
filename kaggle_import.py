import psycopg2
import csv


CSV = 'cs_hltv_data.csv'

username = 'borysenko'
password = 'admin'
database = 'ddd'
host = 'localhost'
port = '5432'

query_0 = '''
CREATE TABLE Match_csgo_2
(
  match_id         int       UNIQUE NOT NULL,
  first_team_name  char(50)  NOT NULL,
  second_team_name  char(50)  NOT NULL,
  date        date       NOT NULL,
  first_team_total_score    int       NOT NULL,
  second_team_total_score    int       NOT NULL,
  M1 char(50)  NOT NULL,
  M2 char(50)  NOT NULL,
  M3 char(50)  NOT NULL,
  CONSTRAINT PK_Match2 PRIMARY KEY (match_id)
);
'''

query_1 = '''
DELETE FROM Match_csgo_2
'''

query_2 = '''
INSERT INTO Match_csgo_2 (match_id, first_team_name, second_team_name,date,first_team_total_score,second_team_total_score,M1,M2,M3 ) VALUES (%s, %s, %s, %s, %s, %s,%s, %s,%s)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)


with conn:

    cur = conn.cursor()

    cur.execute(query_0)
    cur.execute(query_1)

    with open(CSV, 'r') as inf:
        data = csv.DictReader(inf)
        for i, row in enumerate(data):
            temp = (row[""], row["first_team"], row["second_team"], row["date"],row["first_team_total_score"],row["second_team_total_score"],row["M1"],row["M2"],row["M3"])
            print(temp)
            cur.execute(query_2, temp)
    conn.commit()