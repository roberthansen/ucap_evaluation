
import sys
import yaml
from pathlib import Path
from datetime import datetime as dt,timedelta as td

sys.path=[str(Path().cwd())] + sys.path
from src.ezdb_connection.sql_strs import get_economic_bid
from src.ezdb_connection.ezdb_connection \
    import EZBDConnection

if __name__=='__main__':
    with open('config/config.yaml','r') as f:
        config = yaml.safe_load(f)
    with open('config/login.yaml','r') as f:
        login_credentials = yaml.safe_load(f)

    start_datetime = dt(min(config['ucap_analysis']['years']),1,1,0,0)
    end_datetime = dt(max(config['ucap_analysis']['years'])+1,1,1,0,0)

    sql_str = get_economic_bid(start_datetime=start_datetime,end_datetime=end_datetime)

    # Retrieve economic bid data from EZDB:
    ezdbc = EZBDConnection(login_credentials)
    economic_bid_data = ezdbc.execute_query(sql_str)

    # Identify hours when Dispatch Price > Bid Price:
    economic_bid_data.loc[
        :,
        [
            'RTM_DISPATCH_QUANTITY',
            'RTM_DISPATCH_PRICE',
            'RTM_BID_QUANTITY',
            'RTM_BID_PRICE',
            'DAM_DISPATCH_QUANTITY',
            'DAM_DISPATCH_PRICE',
            'DAM_BID_QUANTITY',
            'DAM_BID_PRICE',
            'DAM_SELFSCHEDMW',
            'RUC_DISPATCH_QUANTITY'
        ]
    ].fillna(0,inplace=True)
    economic_bid_data.loc[:,'DEMAND HOUR'] = economic_bid_data.loc[:,'RTM_DISPATCH_PRICE']>economic_bid_data.loc[:,'RTM_BID_PRICE']
    resource_level_demand_hours = economic_bid_data.loc[:,['ResID','DateTime','DEMAND HOUR']]
    resource_level_demand_hours.rename(columns={'ResID':'RESOURCE ID','DateTime':'START DATETIME'},inplace=True)
    resource_level_demand_hours.loc[:,'END DATETIME'] = resource_level_demand_hours.loc[:,'START DATETIME'] + td(hours=1)
    resource_level_demand_hours = resource_level_demand_hours.loc[:,['RESOURCE ID','START DATETIME','END DATETIME','DEMAND HOUR']]
    resource_level_demand_hours.to_parquet(Path(config['demand_hours_analysis']['resource_demand_hours_path']),index=False)
