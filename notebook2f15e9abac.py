# %% [code]
# This Python 3 environment comes with many helpful analytics libraries installed
# It is defined by the kaggle/python Docker image: https://github.com/kaggle/docker-python
# For example, here's several helpful packages to load

import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory

import os
for dirname, _, filenames in os.walk('/kaggle/input'):
    for filename in filenames:
        print(os.path.join(dirname, filename))

# You can write up to 20GB to the current directory (/kaggle/working/) that gets preserved as output when you create a version using "Save & Run All" 
# You can also write temporary files to /kaggle/temp/, but they won't be saved outside of the current sessio

#Import csv file
players = pd.read_csv("/kaggle/input/player-scores/players.csv")
clubs = pd.read_csv("/kaggle/input/player-scores/clubs.csv")
competitions = pd.read_csv("/kaggle/input/player-scores/competitions.csv")
appearances = pd.read_csv("/kaggle/input/player-scores/appearances.csv")
games = pd.read_csv("/kaggle/input/player-scores/games.csv")
pd.options.display.float_format = '{:.2f}'.format
# import sqlalchemy and create a sqlite engine
from sqlalchemy import create_engine
from sqlalchemy import text
engine = create_engine('sqlite://', echo=False)

players.to_sql("players", con=engine)
clubs.to_sql("clubs", con=engine)
competitions.to_sql("competitions", con=engine)
appearances.to_sql("appearances", con=engine)
games.to_sql("games", con=engine)

with engine.connect() as conn:
    query = conn.execute(text("""SELECT 
        t1.player_name, 
        t1.country_of_citizenship,
        t1.position,
        t1.sub_position, 
        t1.sum_minutes_played, 
        t1.sum_goals, 
        t1.sum_assists, 
        t1.market_value_in_eur, 
        t1.GoalsPerMinute, 
        t1.AssistsPerMinute,
        t1.LoseGoalsPerMinute,
        PERCENT_RANK() OVER (PARTITION BY t1.sub_position ORDER BY t1.GoalsPerMinute) AS GoalsPercentile,
        PERCENT_RANK() OVER (PARTITION BY t1.sub_position ORDER BY t1.AssistsPerMinute) AS AssistsPercentile,
        PERCENT_RANK() OVER (PARTITION BY t1.sub_position ORDER BY t1.LoseGoalsPerMinute) AS LoseGoalsPercentile
    FROM (
        SELECT 
            player_name, 
            country_of_citizenship,
            position,
            sub_position, 
            SUM(minutes_played) AS sum_minutes_played, 
            SUM(goals) AS sum_goals, 
            SUM(assists) AS sum_assists,
            SUM(games.away_club_goals),
            market_value_in_eur, 
            COALESCE(SUM(minutes_played)/NULLIF(SUM(goals),0), 9999) AS GoalsPerMinute, 
            COALESCE(SUM(minutes_played)/NULLIF(SUM(assists),0), 9999) AS AssistsPerMinute, 
            COALESCE(SUM(minutes_played)/NULLIF(SUM(games.away_club_goals),0), 9999) AS LoseGoalsPerMinute 
        FROM players 
        INNER JOIN games ON appearances.game_id = games.game_id 
        INNER JOIN appearances ON appearances.player_id = players.player_id 
        WHERE games.season = '2021' 
        GROUP BY player_name, sub_position 
        HAVING SUM(minutes_played) >= 1500 
    ) AS t1
    ORDER BY  t1.GoalsPerMinute"""))

df = pd.DataFrame(query.fetchall())
df.columns = query.keys()

df