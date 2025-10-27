import sys
import yaml
import pandas as pd
from datetime import date as d,time as t,datetime as dt,timedelta as td
from pathlib import Path

sys.path=[str(Path().cwd())] + sys.path
from src.ucap_evaluator.ucap_evaluator import UCAPEvaluator
from src.utils.datetime_functions import select_hours_within_datetime_range,coalesce_hour_filter
from src.utils.string_functions import replace_template_placeholders

def evaluate_ucap():
    '''
    Calculates the equivalent forced outage rates for all hours (EFOR) and
    demand hours (EFORd) within the years and seasons specified in the
    configuration file.
    '''
    with open('config/config.yaml','r') as f:
        config = yaml.safe_load(f)

    ucap_evaluator = UCAPEvaluator(config)

    years = config['ucap_analysis']['years']
    seasons = config['ucap_analysis']['seasons'].items()
    df_out = pd.DataFrame()
    for year in years:
        for season in seasons:
            season_name = season[0]
            season_bounds = season[1]
            start_datetimes = [dt.strptime(x[0],'%b %d').replace(year=year) for x in season_bounds]
            end_datetimes = [dt.strptime(x[1],'%b %d').replace(year=year)+td(days=1) for x in season_bounds]

            # Calculate Equivalent Forced Outage Rates during Demand Hours
            # (EFORd):
            hour_filter = ucap_evaluator.grid_hour_filter.copy()
            hour_filter = pd.concat([
                select_hours_within_datetime_range(
                    start_datetime,
                    end_datetime,
                    hour_filter
                )
                for start_datetime,end_datetime in zip(start_datetimes,end_datetimes)
            ],ignore_index=True)
            hour_filter = coalesce_hour_filter(hour_filter)
            hour_filter = hour_filter.loc[hour_filter.loc[:,'DEMAND HOUR'],:]
            df = ucap_evaluator.calculate_equivalent_forced_outage_rate_during_shared_demand_hours(hour_filter)
            original_columns = df.columns
            df.loc[:,'YEAR'] = year
            df.loc[:,'SEASON'] = season_name
            df_out = pd.concat([df_out,df.loc[:,['YEAR','SEASON']+list(original_columns)]])

    output_path = Path(replace_template_placeholders(
        config['ucap_analysis']['results']['outage_path_template'],
        {'years' : f'{years[0]}-{years[-1]}'}
    ))
    df_out.to_csv(output_path,index=False)

if __name__=='__main__':
    evaluate_ucap()