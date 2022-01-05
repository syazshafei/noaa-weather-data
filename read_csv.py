import pandas as pd
from sqlalchemy import create_engine
file = '/home/syazwan/weather/2015.csv'

print pd.read_csv(file, nrows=5)

eng = create_engine('postgresql+psycopg2://postgres:1234@localhost/weather')

chunksize = 100000
i = 0
j = 1
for df in pd.read_csv(file, chunksize=chunksize, iterator=True):
      df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
      df.index += j
      i+=1
      df.to_sql('table', eng, if_exists='append')
      j = df.index[-1] + 1


df = pd.read_sql_query('SELECT * FROM "table"', eng)