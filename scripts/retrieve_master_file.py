import sys
import yaml
import psycopg2
import pandas as pd
from pathlib import Path

sys.path=[str(Path().cwd())] + sys.path
from src.ezdb_connection.ezdb_connection import EZBDConnection
from src.ezdb_connection.sql_strs import get_master_file

def retrieve_master_file():
    with open('config/config.yaml','r') as f:
        config = yaml.safe_load(f)

    with open('config/login.yaml','r') as f:
        login_credentials = yaml.safe_load(f)

    sql_str = get_master_file()

    ezdb = EZBDConnection(login_credentials)
    results = ezdb.execute_query(sql_str)
    results.loc[:,'RMTG_ON_PEAK_EXPIRE_DT'] = results.loc[:,'RMTG_ON_PEAK_EXPIRE_DT'].to_numpy('datetime64')
    results.loc[:,'RMTG_OFF_PEAK_EXPIRE_DT'] = results.loc[:,'RMTG_OFF_PEAK_EXPIRE_DT'].to_numpy('datetime64')

    results.to_parquet(Path(config['caiso_master_file']['download_path']))

if __name__=='__main__':
    retrieve_master_file()