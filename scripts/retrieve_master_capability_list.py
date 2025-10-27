import sys
import yaml
import psycopg2
import pandas as pd
from pathlib import Path

sys.path=[str(Path().cwd())] + sys.path
from src.ezdb_connection.ezdb_connection import EZBDConnection
from src.ezdb_connection.sql_strs import get_master_capability_list

def retrieve_master_capability_list():
    with open('config/config.yaml','r') as f:
        config = yaml.safe_load(f)

    with open('config/login.yaml','r') as f:
        login_credentials = yaml.safe_load(f)

    sql_str = get_master_capability_list()

    ezdb = EZBDConnection(login_credentials)
    results = ezdb.execute_query(sql_str)
    results.loc[:,'CommercialOperDate'] = results.loc[:,'CommercialOperDate'].to_numpy('datetime64')

    results.to_parquet(Path(config['caiso_master_capability_list']['download_path']))

if __name__=='__main__':
    retrieve_master_capability_list()