import psycopg2
import csv
import pandas as pd


CSV = 'cs_hltv_data.csv'

username = 'borysenko'
password = 'admin'
database = 'ddd'
host = 'localhost'
port = '5432'

query_1_0 = '''
CREATE TABLE Player_ex_csv
(
  match_id         int	NOT NULL,
  player_name	char(50)	 NOT NULL,
  ct_kd    char(50)       NOT NULL,
  t_kd    char(50)       NOT NULL
);
'''
query_1 = '''
INSERT INTO Player_ex_csv (match_id, player_name,ct_kd,t_kd) VALUES (%s, %s, %s, %s)
'''

query_2_0 = '''
CREATE TABLE Match_csgo_ex_csv
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
  CONSTRAINT PK_Match3 PRIMARY KEY (match_id)
);
'''


query_2 = '''
INSERT INTO Match_csgo_ex_csv (match_id, first_team_name, second_team_name,date,first_team_total_score,second_team_total_score,M1,M2,M3 ) VALUES (%s, %s, %s, %s, %s, %s,%s, %s,%s)
'''

query_3_0 = '''
CREATE TABLE Team_ex_csv
(
  team_name  char(50)	 UNIQUE NOT NULL,
  win_games     int      NULL,
  first_picks     int      NULL,
  CONSTRAINT PK_Team2 PRIMARY KEY (team_name)
);

'''

query_3 = '''
INSERT INTO Team_ex_csv (team_name, win_games, first_picks) VALUES (%s, %s, %s)
'''


conn = psycopg2.connect(user=username, password=password, dbname=database)


with conn:

    cur = conn.cursor()
    cur.execute(query_1_0)
    cur.execute(query_2_0)
    cur.execute(query_3_0)
    list_players = ["first_team_P", "second_team_P"]
    first_pick = []
    first_team = []
    second_team = []
    first_team_won = []

    with open(CSV, 'r') as f:
        data = csv.DictReader(f)
        for row in data:
            ## Виконня заповнення таблиці Match_csgo_2
            temp = (row[""], row["first_team"], row["second_team"], row["date"], row["first_team_total_score"], row["second_team_total_score"], row["M1"], row["M2"], row["M3"])
            cur.execute(query_2, temp)
            ## Додаткові данні для третього запиту для заповнення таблиці Team2
            first_pick.append(int(row["first_pick_by_first_team"]))
            first_team.append(row["first_team"])
            second_team.append(row["second_team"])
            first_team_won.append(int(row["first_team_won"]))
            ## Виконня заповнення таблиці Player2
            for i in list_players:
                for j in range(1, 6):
                    temp = (row[""], row[i+str(j)], row[i+str(j)+"_CT_KD"], row[i+str(j)+"_T_KD"])
                    cur.execute(query_1, temp)
    ## Отримання даних для третього запиту для заповнення таблиці Team2
    temp2 = pd.DataFrame({"t1":first_team,"t2":second_team, "t1w": first_team_won, "t1p":first_pick})
    onf1wi = list(temp2[["t1", "t1w"]].groupby("t1").aggregate(sum).index)
    onf1wc = list(map(lambda x: x[0], list(temp2[["t1", "t1w"]].groupby("t1").aggregate(sum).values)))
    onf2wi = list(temp2[temp2['t1w'] == 0].groupby("t2").count()["t1w"].index)
    onf2wc = list(temp2[temp2['t1w'] == 0].groupby("t2").count()["t1w"].values)
    team = onf1wi+onf2wi
    team_win = onf1wc+onf2wc
    win = pd.DataFrame({"team":team, "win":team_win}).groupby("team").aggregate(sum)
    t1f1 = temp2[["t1", "t1p"]][temp2['t1w'] == 1].groupby("t1").aggregate(sum)
    temp3 = temp2[temp2['t1w'] == 0]
    t2f1 = temp3[temp2['t1p'] == 0].groupby("t2").count()['t1']
    t1f1i = list(t1f1.index)
    t1f1c = list(map(lambda x:x[0],list(t1f1.values)))
    t2f1i = list(t2f1.index)
    t2f1c = list(t2f1.values)
    team = t1f1i + t2f1i
    winf1 = t1f1c + t2f1c
    winf = pd.DataFrame({"team":team, "winf":winf1 }).groupby("team").aggregate(sum)
    res = pd.concat([win, winf], axis=1).fillna(0)
    ## Виконня заповнення таблиці Team2
    for index, row in res.iterrows():
        cur.execute(query_3, (index,row["win"],row["winf"]))
    conn.commit()


    #cur.execute(query_2)
