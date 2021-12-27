import psycopg2
import matplotlib.pyplot as plt
import numpy as np


username = 'borysenko'
password = 'admin'
database = 'ddd'
host = 'localhost'
port = '5432'


query_1 = '''
CREATE VIEW KDStatPlayer AS
SELECT player_name, ct_kd FROM Player WHERE match_id = 0
'''

query_2_1 = '''
CREATE VIEW FirstMapPick AS
SELECT M1, COUNT(*) AS count_map FROM Match_csgo GROUP BY M1
'''
query_2_2 = '''
CREATE VIEW SecondMapPick AS
SELECT M2, COUNT(*) AS count_map FROM Match_csgo GROUP BY M2
'''
query_2_3 = '''
CREATE VIEW ThirdMapPick AS
SELECT M3, COUNT(*) AS count_map FROM Match_csgo GROUP BY M3
'''

query_3 = '''
CREATE VIEW CountWinningMatches AS
SELECT team_name, win_games FROM Team WHERE win_games != 0
'''

con = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)



with con:
    cur = con.cursor()

    x = []
    y = []

    cur.execute('DROP VIEW IF EXISTS KDStatPlayer')
    cur.execute(query_1)
    cur.execute('SELECT * FROM KDStatPlayer')

    for row in cur:
        x.append(row[0].replace(" ", ""))
        y.append(round(eval(row[1].replace(" ", "").replace("-", "/")), 2))
    fig, ax = plt.subplots()
    fig.set_figheight(10)
    fig.set_figwidth(10)
    plt.bar(x, y)
    plt.xticks(rotation=45)
    plt.yticks(np.arange(0, max(y)+0.1, 0.1))
    ax.set_title('KillDeath статистика кожного гравця у матчі')
    plt.xlabel('Гравець')
    plt.ylabel('Відношення його вбивств до смертей')
    plt.show()

    x = []
    y = []

    cur.execute('DROP VIEW IF EXISTS FirstMapPick')
    cur.execute(query_2_1)
    cur.execute('SELECT * FROM FirstMapPick')

    for row in cur:
        x.append(row[1])
        y.append(row[0].replace(" ", ""))
    fig, ax = plt.subplots()
    ax.set_title('Cтатистика вибору першої карти у вигляді кругової діаграми')
    ax.pie(x, labels=y, autopct='%1.1f%%')
    ax.axis("equal")
    plt.show()

    x = []
    y = []

    cur.execute('DROP VIEW IF EXISTS SecondMapPick')
    cur.execute(query_2_2)
    cur.execute('SELECT * FROM SecondMapPick')

    for row in cur:
        x.append(row[1])
        y.append(row[0].replace(" ", ""))
    fig, ax = plt.subplots()
    ax.set_title('Cтатистика вибору другої карти у вигляді кругової діаграми')
    ax.pie(x, labels=y, autopct='%1.1f%%')
    ax.axis("equal")
    plt.show()

    x = []
    y = []

    cur.execute('DROP VIEW IF EXISTS ThirdMapPick')
    cur.execute(query_2_3)
    cur.execute('SELECT * FROM ThirdMapPick')

    for row in cur:
        x.append(row[1])
        y.append(row[0].replace(" ", ""))
    fig, ax = plt.subplots()
    ax.set_title('Cтатистика вибору третьої карти у вигляді кругової діаграми')
    ax.pie(x, labels=y, autopct='%1.1f%%')
    ax.axis("equal")
    plt.show()

    x = []
    y = []

    cur.execute('DROP VIEW IF EXISTS CountWinningMatches')
    cur.execute(query_3)
    cur.execute('SELECT * FROM CountWinningMatches')

    for row in cur:
        x.append(row[0].replace(" ", ""))
        y.append(row[1])
    fig, ax = plt.subplots()
    fig.set_figheight(10)
    fig.set_figwidth(10)
    plt.bar(x, y)
    plt.xticks(rotation=45)
    plt.yticks(np.arange(0, max(y) + 0.1, 1))
    ax.set_title('Стовпчикова діаграма команд, що вигравала в 6 матчах')
    plt.xlabel('Команда')
    plt.ylabel('Виграні матчі')
    plt.show()


