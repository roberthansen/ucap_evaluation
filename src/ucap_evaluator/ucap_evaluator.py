import pandas as pd
from pathlib import Path
from datetime import date as d,time as t,datetime as dt,timedelta as td
from src.logging.logging import TextLogger
from src.utils.datetime_functions import datetime_range_overlap,hour_filter_overlap,select_hours_within_datetime_range,coalesce_hour_filter

class UCAPEvaluator:
    '''
    A class to manage analysis of curtailment data to evaluate UCAP values for
    individual resources.
    '''
    def __init__(self,config:dict):
        self.combined_reports_path = Path(config['caiso_curtailment_reports']['combined_reports_path'])
        self.status_logger = TextLogger(
            cli_logging_criticalities=['INFORMATION','WARNING','ERROR'],
            file_logging_criticalities=['WARNING','ERROR'],
            log_path=config['ucap_analysis']['text_log_path']
        )
        self.curtailment_data = pd.read_parquet(self.combined_reports_path)
        self.natures_of_work = config['ucap_analysis']['natures_of_work']
        self.grid_hour_filter = pd.read_csv(config['ucap_analysis']['hour_filter_path'],parse_dates=[0,1])
        self.resource_hour_filter = pd.read_parquet(config['demand_hours_analysis']['resource_demand_hours_path'])
    
    def calculate_equivalent_forced_outage_rates_by_date_range(self,start_date:d,end_date:d):
        '''
        Filters curtailment data for the input range of dates, then evaluates
        the equivalent forced outage rate (EFOR) for the month based on curtailment
        data given the nature-of-work codes included in the configuration file.

            parameters:
                start_date - a datetime.date object specifying the initial date
                    of a range of dates across which to evaluate the EFOR for
                    each resource in the curtailment data
                end_date - a datetime.date object specifying the final date of a
                    range of dates across which to evaluate the EFOR for each
                    resource in the curtailment data
        '''
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        self.status_logger.log(f'Calculating EFOR Within Date Range {start_date_str} to {end_date_str}.',criticality='INFORMATION')

        # Copy curtailment data for analysis:
        df = self.curtailment_data.copy()

        # Remove any curtailment reports without an end date time:
        df = df.loc[df.loc[:,'CURTAILMENT END DATE TIME'].notnull(),:]

        # Filter curtailment data for reports containing dates within input
        # range:
        self.status_logger.log('\tFiltering curtailment reports.',criticality='INFORMATION')
        df.loc[:,'INCLUDE'] = df.apply(
            lambda r: \
            (r.loc['CURTAILMENT START DATE TIME']<dt.combine(end_date,t(0,0,0))+td(days=1)) \
            and (r.loc['CURTAILMENT END DATE TIME']>=dt.combine(start_date,t(0,0,0))),
            axis='columns',
            result_type='expand'
        )
        df = df.loc[df.loc[:,'INCLUDE'],:]

        # Filter curtailment data for forced outages and nature-of-work codes
        # listed in the configuration file:
        df = df.loc[(df.loc[:,'OUTAGE TYPE']=='FORCED'),:]
        if self.natures_of_work is not None:
            df.loc[:,'INCLUDE'] = df.apply(
                lambda r: r.loc['NATURE OF WORK'] in self.natures_of_work,
                axis='columns',
                result_type='expand'
            )
            df = df.loc[df.loc[:,'INCLUDE'],:]

        # Get most recent curtailment report for each MRID:
        current_reports = df.loc[:,[
            'OUTAGE MRID',
            'REPORT DATE'
        ]].groupby('OUTAGE MRID').max().reset_index().apply(
            lambda r:(r.loc['OUTAGE MRID'],r.loc['REPORT DATE']),
            axis='columns',
            result_type='reduce'
        )
        df = df.set_index(['OUTAGE MRID','REPORT DATE']).loc[current_reports,:].reset_index()

        # Calculate reported outage hours within date range and selected nature-
        # of-work codes:
        self.status_logger.log('\tCalculating outage rates within date range.')
        df = df.loc[
            (df.loc[:,'CURTAILMENT START DATE TIME']<=dt.combine(end_date,t(0,0,0))+td(days=1)) \
            * (df.loc[:,'CURTAILMENT END DATE TIME']>=dt.combine(start_date,t(0,0,0)))
        ]
        df.loc[:,'APPLICABLE OUTAGE HOURS'] = df.apply(
            lambda r:datetime_range_overlap(
                r.loc['CURTAILMENT START DATE TIME'],
                r.loc['CURTAILMENT END DATE TIME'],
                dt.combine(start_date,t(0,0,0)),
                dt.combine(end_date,t(0,0,0)) + td(days=1)
            ),
            axis='columns',
            result_type='expand'
        )

        # Calculate curtailed capacity * duration in MWh
        df.loc[:,'APPLICABLE OUTAGE MWH'] = df.loc[:,'CURTAILMENT MW'] \
            * df.loc[:,'APPLICABLE OUTAGE HOURS']

        # Calculate maximum capacity * time range in MWh
        df.loc[:,'APPLICABLE PMAX MWH'] = df.loc[:,'RESOURCE PMAX MW'] \
            * (
                dt.combine(end_date,t(0,0,0)) + td(days=1)
                - dt.combine(start_date,t(0,0,0)) \
            ).total_seconds() / 3600
        
        # Aggregate by resource id:
        df = df.loc[:,['RESOURCE ID','APPLICABLE PMAX MWH','APPLICABLE OUTAGE MWH']].groupby('RESOURCE ID').agg({
            'APPLICABLE PMAX MWH': 'max',
            'APPLICABLE OUTAGE MWH': 'sum'
        }).reset_index()

        # Calculate forced outage rate for selected time range as outage MWh/
        # maximum possible MWh:
        df.loc[:,'EQUIVALENT FORCED OUTAGE RATE'] = df.loc[:,'APPLICABLE OUTAGE MWH'] / df.loc[:,'APPLICABLE PMAX MWH']
        return df
    
    def calculate_equivalent_forced_outage_rate_during_resource_demand_hours(self,resource_hour_filter:list):
        '''
        Filters curtailment data for the input range of dates, then evaluates
        the forced outage rate for demand periods based on a set of resource-
        level demand hours.

            parameters:
                hour_filter - a pandas dataframe with columns labeled
                    'RESOURCE ID','DATETIME' and 'INCLUDE' indicating selected
                    hours, generally expected to constitute a year of hours.
            
            returns:
                a pandas dataframe with each resource id and corresponding
                equivalent forced outage rate during the demand hours identified
                in the input hour filter.
        '''
        self.status_logger.log('Calculating EFORd with Individual Resource Demand Period Blocks.',criticality='INFORMATION')
        # Copy curtailment data for analysis:
        df = self.curtailment_data.copy()

        # Remove any curtailment reports without an end date time:
        df = df.loc[df.loc[:,'CURTAILMENT END DATE TIME'].notnull(),:]

        # Breakup resource_hour_filter into separate dataframes for each resource:
        hour_filter_dict = {k:resource_hour_filter.loc[resource_hour_filter.loc[:,'RESOURCE ID']==k,['START DATETIME','END DATETIME','INCLUDE']] for k in hour_filter.loc[:,'RESOURCE ID'].unique()}

        # Filter curtailment data for reports containing dates within input
        # range:
        def f(r):
            resource_id = r.loc['RESOURCE ID']
            hf = hour_filter_dict[resource_id]
            start_datetime = hf.loc[:,'START DATETIME'].min().to_pydatetime()
            end_datetime = hf.loc[:,'END DATETIME'].max().to_pydatetime()
            return datetime_range_overlap(
                start_datetime,
                end_datetime,
                r.loc['CURTAILMENT START DATE TIME'],
                r.loc['CURTAILMENT END DATE TIME']
            )>0
        df.loc[:,'INCLUDE'] = df.apply(
            f,
            axis='columns',
            result_type='expand'
        )
        df = df.loc[df.loc[:,'INCLUDE'],:]

        # Filter curtailment data for forced outages and nature-of-work codes
        # listed in the configuration file:
        df = df.loc[(df.loc[:,'OUTAGE TYPE']=='FORCED'),:]
        if self.natures_of_work is not None:
            df.loc[:,'INCLUDE'] = df.apply(
                lambda r: r.loc['NATURE OF WORK'] in self.natures_of_work,
                axis='columns',
                result_type='expand'
            )
            df = df.loc[df.loc[:,'INCLUDE'],:]

        # Get most recent curtailment report for each MRID:
        current_reports = df.loc[:,[
            'OUTAGE MRID',
            'REPORT DATE'
        ]].groupby('OUTAGE MRID').max().reset_index().apply(
            lambda r:(r.loc['OUTAGE MRID'],r.loc['REPORT DATE']),
            axis='columns',
            result_type='reduce'
        )
        df = df.set_index(['OUTAGE MRID','REPORT DATE']).loc[current_reports,:].reset_index()

        # Calculate reported outage hours within date range and hour filter:
        df = df.loc[
            (df.loc[:,'CURTAILMENT START DATE TIME']<=resource_hour_filter.loc[:,'END DATETIME'].max()) \
            * (df.loc[:,'CURTAILMENT END DATE TIME']>=resource_hour_filter.loc[:,'START DATETIME'].min()),
            :
        ]
        def f(r):
            resource_id = r.loc['RESOURCE ID']
            hf = hour_filter_dict[resource_id]
            return hour_filter_overlap(
                r.loc['CURTAILMENT START DATE TIME'],
                r.loc['CURTAILMENT END DATE TIME'],
                hour_filter
            )
        df.loc[:,'APPLICABLE OUTAGE HOURS'] = df.apply(
            f,
            axis='columns',
            result_type='expand'
        )

        # Calculate curtailed capacity * duration in MWh
        self.status_logger.log('\tCalculating outage rates within hour filter.')
        df.loc[:,'APPLICABLE OUTAGE MWH'] = df.loc[:,'CURTAILMENT MW'] \
            * df.loc[:,'APPLICABLE OUTAGE HOURS']

        # Calculate maximum capacity * time range in MWh
        df.loc[:,'APPLICABLE PMAX MWH'] = df.loc[:,'RESOURCE PMAX MW'] \
            * (
                 hour_filter.loc[:,'END DATETIME'] \
                - hour_filter.loc[:,'START DATETIME']
            ).sum().total_seconds() / 3600
        
        # Aggregate by resource id:
        df = df.loc[:,['RESOURCE ID','APPLICABLE PMAX MWH','APPLICABLE OUTAGE MWH']].groupby('RESOURCE ID').agg({
            'APPLICABLE PMAX MWH': 'max',
            'APPLICABLE OUTAGE MWH': 'sum'
        }).reset_index()

        # Calculate forced outage rate for selected time range as :
        df.loc[:,'EQUIVALENT FORCED OUTAGE RATE DURING DEMAND'] = df.loc[:,'APPLICABLE OUTAGE MWH'] / df.loc[:,'APPLICABLE PMAX MWH']
        return df

    def calculate_equivalent_forced_outage_rate_during_grid_demand_hours(self,shared_hour_filter:list):
        '''
        Filters curtailment data for the input range of dates, then evaluates
        the forced outage rate during demand periods based on a set of shared
        demand hours specified in the input shared_hour_filter.

            parameters:
                shared_hour_filter - a pandas dataframe with columns labeled
                    'DATETIME' and 'INCLUDE' indicating selected hours,
                    generally expected to constitute a year of hours.
            
            returns:
                a pandas dataframe with each resource id and corresponding
                equivalent forced outage rate during the demand hours identified
                in the input hour filter.
        '''
        block_count = len(shared_hour_filter)
        self.status_logger.log(f'Calculating EFORd with {block_count} Demand Period Blocks.',criticality='INFORMATION')
        # Copy curtailment data for analysis:
        df = self.curtailment_data.copy()

        # Remove any curtailment reports without an end date time:
        df = df.loc[df.loc[:,'CURTAILMENT END DATE TIME'].notnull(),:]

        # Filter curtailment data for reports containing dates within input
        # range:
        self.status_logger.log('\tFiltering curtailment reports.',criticality='INFORMATION')
        start_datetime = shared_hour_filter.loc[:,'START DATETIME'].min().to_pydatetime()
        end_datetime = shared_hour_filter.loc[:,'END DATETIME'].max().to_pydatetime()
        df.loc[:,'INCLUDE'] = df.apply(
            lambda r: datetime_range_overlap(
                        start_datetime,
                        end_datetime,
                        r.loc['CURTAILMENT START DATE TIME'],
                        r.loc['CURTAILMENT END DATE TIME']
            )>0,
            axis='columns',
            result_type='expand'
        )
        df = df.loc[df.loc[:,'INCLUDE'],:]

        # Filter curtailment data for forced outages and nature-of-work codes
        # listed in the configuration file:
        df = df.loc[(df.loc[:,'OUTAGE TYPE']=='FORCED'),:]
        if self.natures_of_work is not None:
            df.loc[:,'INCLUDE'] = df.apply(
                lambda r: r.loc['NATURE OF WORK'] in self.natures_of_work,
                axis='columns',
                result_type='expand'
            )
            df = df.loc[df.loc[:,'INCLUDE'],:]

        # Get most recent curtailment report for each MRID:
        current_reports = df.loc[:,[
            'OUTAGE MRID',
            'REPORT DATE'
        ]].groupby('OUTAGE MRID').max().reset_index().apply(
            lambda r:(r.loc['OUTAGE MRID'],r.loc['REPORT DATE']),
            axis='columns',
            result_type='reduce'
        )
        df = df.set_index(['OUTAGE MRID','REPORT DATE']).loc[current_reports,:].reset_index()

        # Calculate reported outage hours within date range and hour filter:
        df = df.loc[
            (df.loc[:,'CURTAILMENT START DATE TIME']<=shared_hour_filter.loc[:,'END DATETIME'].max()) \
            * (df.loc[:,'CURTAILMENT END DATE TIME']>=shared_hour_filter.loc[:,'START DATETIME'].min()),
            :
        ]
        df.loc[:,'APPLICABLE OUTAGE HOURS'] = df.apply(
            lambda r:hour_filter_overlap(
                r.loc['CURTAILMENT START DATE TIME'],
                r.loc['CURTAILMENT END DATE TIME'],
                shared_hour_filter
            ),
            axis='columns',
            result_type='expand'
        )

        # Calculate curtailed capacity * duration in MWh
        self.status_logger.log('\tCalculating outage rates within hour filter.')
        df.loc[:,'APPLICABLE OUTAGE MWH'] = df.loc[:,'CURTAILMENT MW'] \
            * df.loc[:,'APPLICABLE OUTAGE HOURS']

        # Calculate maximum capacity * time range in MWh
        df.loc[:,'APPLICABLE PMAX MWH'] = df.loc[:,'RESOURCE PMAX MW'] \
            * (
                 shared_hour_filter.loc[:,'END DATETIME'] \
                - shared_hour_filter.loc[:,'START DATETIME']
            ).sum().total_seconds() / 3600
        
        # Aggregate by resource id:
        df = df.loc[:,['RESOURCE ID','APPLICABLE PMAX MWH','APPLICABLE OUTAGE MWH']].groupby('RESOURCE ID').agg({
            'APPLICABLE PMAX MWH': 'max',
            'APPLICABLE OUTAGE MWH': 'sum'
        }).reset_index()

        # Calculate forced outage rate for selected time range as :
        df.loc[:,'EQUIVALENT FORCED OUTAGE RATE DURING DEMAND'] = df.loc[:,'APPLICABLE OUTAGE MWH'] / df.loc[:,'APPLICABLE PMAX MWH']
        return df