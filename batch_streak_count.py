import psycopg2
import io
import pandas as pd
import numpy as np
from datetime import datetime
from sqlalchemy import create_engine

MAX_CURRENT_STREAK_WINDOW = 2

def build_streak_counts(engine, userId=None):
  query = 'select "userId", "createdAt" as t from "CheckIns"'
  if userId is not None:
    query = f'select "userId", "trackId", "createdAt" as t from "CheckIns" where "userId"=\'{userId}\''

  data = pd.read_sql_query(query, con=engine)
  if data.size == 0:
    return 0

  data['t'] = pd.to_datetime(data['t'].dt.date, errors='coerce')

  df = (
    pd.DataFrame(data)
      .groupby(by=['userId', 't'])
      .size()
      .reset_index(name='totalCheckIns')
  )

  gids = (
    df[['userId', 't']]
      .groupby('userId')
      .diff()
      .t
      .dt
      .days
      .ne(1)
      .cumsum()
  )

  df['streakGroupId'] = gids
  df['streak'] = df.groupby('streakGroupId').cumcount() + 1

  gb = df.groupby(['userId'])
  counts = gb.totalCheckIns.sum().to_frame()

  stats = (counts
      .join(gb.agg({'streak': 'max'}).rename(columns={'streak': 'bestStreak'}))
      .join(gb.agg({'streak': 'last'}).rename(columns={'streak': 'lastStreak'}))
      .join(gb.agg({'t': 'last'}).rename(columns={'t': 'lastCheckedInAt'}))
      .reset_index())

  stats['currentStreak'] = stats['lastStreak']
  stats['createdAt'] = datetime.utcnow()
  stats.loc[pd.datetime.today() - stats['lastCheckedInAt'] > pd.offsets.Day(MAX_CURRENT_STREAK_WINDOW), 'currentStreak'] = 0

  conn = engine.raw_connection()
  cursor = conn.cursor()
  output = io.StringIO()
  stats[['userId', 'bestStreak', 'currentStreak', 'createdAt']].to_csv(output, sep='\t', header=False, index=False)
  output.seek(0)

  print("This job will bulk insert records below")
  print(output.getvalue())

  cursor.copy_from(output, '"UserActivities"', columns=('"userId"', '"bestStreak"', '"currentStreak"', '"createdAt"'))
  conn.commit()
  conn.close()

  return stats.shape[0]
