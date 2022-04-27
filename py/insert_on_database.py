# %%
import pandas as pd
from sqlalchemy import create_engine
import psycopg2

# %%
df = pd.read_pickle(r'../../credenciais.pkl')

# %%
user = df['user'][0]
password = df['password'][0]
host = df['host'][0]
port = df['port'][0]
database = df['database'][0]

# %%
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

# %%
def data_mart_to_database():
    global df_1, df_2
    df_1 = pd.read_parquet(r'../database_dashboard/data_mart_top_20_real_state_funds.parquet')
    df_2 = pd.read_parquet(r'../database_dashboard/data_mart_real_state_date.parquet')
    df_1.to_sql('data_mart_top_20_real_state_funds',schema=None,if_exists='append',index=False,con=engine)
    df_2.to_sql('data_mart_real_state_date',schema=None,if_exists='append',index=False,con=engine)
data_mart_to_database()


