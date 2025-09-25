import sys
import yaml
import pandas as pd
from datetime import date as d,time as t,datetime as dt,timedelta as td
from pathlib import Path

sys.path=[str(Path().cwd())] + sys.path
from src.ucap_evaluator.ucap_evaluator import UCAPEvaluator
from src.utils.datetime_functions import select_hours_within_datetime_range,coalesce_hour_filter
from src.utils.string_functions import replace_template_placeholders

if __name__=='__main__':
    '''
    Calculates the equivalent forced outage rates for all hours (EFOR) and
    demand hours (EFORd) within the years and seasons specified in the
    configuration file.
    '''
    with open('config/config.yaml','r') as f:
        config = yaml.safe_load(f)

    ucap_evaluator = UCAPEvaluator(config)

    # Filter curtailment data for a single resource to evaluate:
    # df = ucap_evaluator.curtailment_data.copy()
    # ucap_evaluator.curtailment_data = df.loc[df.loc[:,'RESOURCE ID']=='CARLS1_2_CARCT1',:]

    for year in config['ucap_analysis']['years']:
        for season in config['ucap_analysis']['seasons'].items():
            season_name = season[0]
            season_bounds = season[1]
            start_datetimes = [dt.strptime(x[0],'%b %d').replace(year=year) for x in season_bounds]
            end_datetimes = [dt.strptime(x[1],'%b %d').replace(year=year) for x in season_bounds]
            # All Hours (EFOR):
            hour_filter = pd.DataFrame({
                'START DATETIME' : start_datetimes,
                'END DATETIME' : end_datetimes,
                'DEMAND HOUR' : [True] * len(start_datetimes)
            })
            df = ucap_evaluator.calculate_equivalent_forced_outage_rate_during_grid_demand_hours(hour_filter)
            output_path = Path(replace_template_placeholders(
                config['ucap_analysis']['results']['path_template'],
                {'type' : 'efor', 'season' : season_name, 'year' : str(year)}
            ))
            df.to_csv(output_path,index=False)

            # Grid Demand Hours (EFORd):
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
            df = ucap_evaluator.calculate_equivalent_forced_outage_rate_during_grid_demand_hours(hour_filter)
            output_path = Path(replace_template_placeholders(
                config['ucap_analysis']['results']['path_template'],
                {'type' : 'eford', 'season' : season_name, 'year' : str(year)}
            ))
            df.to_csv(output_path,index=False)

            # Resource-Level Demand Hours (EFORd):
            hour_filter = ucap_evaluator.resource_hour_filter.copy()
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
            df = ucap_evaluator.calculate_equivalent_forced_outage_rate_during_resource_demand_hours(hour_filter)
            output_path = Path(replace_template_placeholders(
                config['ucap_analysis']['results']['path_template'],
                {'type' : 'eford', 'season' : season_name, 'year' : str(year)}
            ))
            
            df.to_csv(output_path,index=False)

