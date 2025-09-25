import sys
import yaml
import unittest
from pathlib import Path
from datetime import datetime as dt

sys.path=['M:\\Users\\RH2\\src\\ucap_evaluation'] + sys.path
from ezdb_connection.sql_strs import get_economic_bid
from src.ezdb_connection.ezdb_connection import EZBDConnection

class TestEconomicBidQuery(unittest.TestCase):
    config_path = Path(r'config/config.yaml')
    login_path = Path(r'config/login.yaml')

    def __init__(self,*args,**kwargs):
        # Modifies the class initializer to incorporate configuration settings.
        super(TestEconomicBidQuery,self).__init__(*args,**kwargs)
        with self.config_path.open('r') as f:
            self.config = yaml.safe_load(f)
        with self.login_path.open('r') as f:
            self.login_credentials = yaml.safe_load(f)
        self.ezdb_connection = EZBDConnection(self.login_credentials)

    def test_economic_bid_query(self):
        #Tests the connection and query to retrieve economic bid data from EZDB.
        start_datetime = dt(2022,7,1,0,0,0)
        end_datetime = dt(2022,8,1,0,0,0)
        sql_str = get_economic_bid(start_datetime=start_datetime,end_datetime=end_datetime)
        ezdb_connection = EZBDConnection(self.login_credentials)
        economic_bid_data = ezdb_connection.execute_query(sql_str)
        print(economic_bid_data)

if __name__=='__main__':
    unittest.main()