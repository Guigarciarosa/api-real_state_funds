# %% [markdown]
# # 1.0 Libs and Functions

# %%
# get libs
from bs4 import BeautifulSoup
import pandas as pd
import requests
import datetime as dt
from datetime import timedelta
#import psycopg2 as psy2
#from sqlalchemy import create_engine
#import pickle

# %%
def config():
    global options, coerce_column_to_numeric, dir_path_saved
    options = pd.options.mode.chained_assignment = None
    def coerce_column_to_numeric(df,column):
        df[column] = df[column].apply(pd.to_numeric, errors='coerce')
    dir_path_saved = r'../database_dashboard/'
config()

# %% [markdown]
# # 2.0 ETL
# - Create datasets with organized data about the real-state funds.
# - Clean all the data from the url https://www.fundsexplorer.com.br/ranking

# %%
def etl_funds():
    print('\n ######################### SOUP ############################')
    def soup():
        global table, site
        # get the url
        url = 'https://www.fundsexplorer.com.br/ranking'
        response = requests.get(url)
        # open the html parser
        site = BeautifulSoup(response.text, 'html.parser')
    soup()
        

    def transform():
        global data
        data = []
        table = site.find(id="table-ranking") 
        table_head = table.find('thead')
        rows = table_head.find_all('tr')
        for row in rows:
            cols = row.find_all('th')
            colsd = [ele.get_text(separator=" ").strip() for ele in cols]
            data.append([ele for ele in colsd])
    # find the table with the funds list
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
    # drop the str 
            colsd = [ele.text.replace('R$','').replace('%','').replace('.','').replace('N/A','').replace(',','.').strip() for ele in cols]
            data.append([ele for ele in colsd])
    transform()

    def data_append():
        global df,hj
        # put the data inside a data frame
        for x in data : df = pd.DataFrame(data=data)
        df.columns = df.iloc[0]
        df = df.drop(index=0)
        hj = dt.date.today()
        df['data_extracao'] = hj
        '''df.rename(columns={'Código do fundo':'codigo_do_fundo', 'Setor':'setor', 'Preço Atual':'preco_atual', 'Liquidez Diária':'liquidez_diaria',
        'Dividendo':'dividendo', 'Dividend Yield':'dividend_yield', 'DY (3M) Acumulado':'dy_3m_acumulado', 'DY (6M) Acumulado':'dy_6m_acumulado',
        'DY (12M) Acumulado':'DY_(12M)_Acumulado', 'DY (3M) Média':'dy_3m_media', 'DY (6M) Média':'dy_6m_media',
        'DY (12M) Média':'dy_12M_media', 'DY Ano':'dy_ano', 'Variação Preço':'variacao_preco', 'Rentab. Período':'rentabilidade_periodo',
        'Rentab. Acumulada':'rentabilidade_acumulada', 'Patrimônio Líq.':'patrimonio_liq', 'VPA':'vpa', 'P/VPA':'p_vpa',
        'DY Patrimonial':'dy_patrimonial', 'Variação Patrimonial':'variacao_patrimonial', 'Rentab. Patr. no Período':'rentabilidade_patrimonial_periodo',
        'Rentab. Patr. Acumulada':'rentabilidade_patrimonial_acumulada', 'Vacância Física':'vacancia_fisica', 'Vacância Financeira':'vacancia_financeira',
        'Quantidade Ativos':'quantidade_ativos', 'Hoje':'hoje'}, inplace=True)'''
        #df.to_parquet(r'../database/funds_database.parquet')
        df.columns = df.columns.str.lower()
        columns = df.columns
        columns = columns.str.replace(' ','_',regex=True)
        columns = columns.str.replace('á','a')
        columns = columns.str.replace('í','i')
        columns = columns.str.replace('ô','o')
        columns = columns.str.replace('ó','o')
        columns = columns.str.replace('â','a')
        columns = columns.str.replace('ç','c')
        columns = columns.str.replace('é','e')
        columns = columns.str.replace('.','',regex=True)
        columns = columns.str.replace('(','',regex=True)
        columns = columns.str.replace(')','',regex=True)
        df.columns = columns
    data_append()

    def save_dataset():
        print('\n All ETL realeased.')
        df.to_parquet(fr'{dir_path_saved}data_mart_all_real_state.parquet',index=False)
        print('\n Data Mart Created')
    save_dataset()

etl_funds()

# %%
def collect_data_from_real_state():
    print('\n Clean the real_state datamart to collect date.')
    global csv,csv_1
    csv = pd.read_csv(r'../databases/fii_real_state_date.csv','utf-8',engine='python')
    csv = csv[';;;;;;;;;'].str.split(';',expand=True)
    csv = csv.loc[31:331]
    csv.rename(columns={0:'codigo_do_fundo',1:'nome_do_fundo',2:'fechamento_em_moeda', 3:'valor_por_cota',4:'yield_mensal',5:'yield_anual',6:'tipo',7:'período_de_referencia',8:'data_base',9:'data_de_pagamento'},inplace=True)
    csv = csv.drop(index=[31,32])
    csv['fechamento_em_moeda'] = csv['fechamento_em_moeda'].str.replace('N/A','0')
    csv['valor_por_cota'] = csv['valor_por_cota'].str.replace(',','.')
    csv['fechamento_em_moeda'] = csv['fechamento_em_moeda'].str.replace(',','.')
    csv['valor_por_cota'] = csv['valor_por_cota'].astype('float64')
    csv['fechamento_em_moeda'] = csv['fechamento_em_moeda'].astype('float64')
    csv['yield_mensal'] = csv['yield_mensal'].str.replace('N/A','0')
    csv['yield_anual'] = csv['yield_anual'].str.replace('N/A','0')
    csv['yield_mensal'] = csv['yield_mensal'].str.replace(',','.')
    csv['yield_anual'] = csv['yield_anual'].str.replace(',','.')
    csv['data_de_pagamento'] = csv['data_de_pagamento'].str.replace('/','-')
    csv['data_de_pagamento'] = csv['data_de_pagamento'].astype('datetime64')
    csv['data_base'] = csv['data_base'].str.replace('/','-')
    csv['data_base'] = csv['data_base'].astype('datetime64')
    csv.to_parquet(r'../database_dashboard/data_mart_real_state_date.parquet',index=False)
collect_data_from_real_state()


