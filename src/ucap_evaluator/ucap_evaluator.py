import pandas as pd
import numpy as np
import multiprocessing as mp
from pathlib import Path
from datetime import date as d,time as t,datetime as dt,timedelta as td
from src.logging.logging import TextLogger
from src.utils.datetime_functions import datetime_range_overlap,hour_filter_overlap,select_hours_within_datetime_range,coalesce_hour_filter
from scripts.retrieve_master_capability_list import retrieve_master_capability_list
from scripts.retrieve_master_file import retrieve_master_file

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
        # self.resource_hour_filter = pd.read_parquet(config['demand_hours_analysis']['resource_demand_hours_path'])

        master_capability_list_path = Path(config['caiso_master_capability_list']['download_path'])
        if not master_capability_list_path.is_file():
            retrieve_master_capability_list()
        self.master_capability_list = pd.read_parquet(master_capability_list_path)

        master_file_path = Path(config['caiso_master_file']['download_path'])
        if not master_file_path.is_file():
            retrieve_master_file()
        self.master_file = pd.read_parquet(master_file_path)
    
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

        # Get prepared curtailment data:
        df = self.prepare_curtailment_data()

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
        df.loc[:,'OUTAGE MWH DURING DEMAND'] = df.apply(
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
        df.loc[:,'OUTAGE MWH DURING DEMAND'] = df.loc[:,'CURTAILMENT MW'] \
            * df.loc[:,'APPLICABLE OUTAGE HOURS']

        # Calculate maximum capacity * time range in MWh
        df.loc[:,'APPLICABLE PMAX MWH'] = df.loc[:,'RESOURCE PMAX MW'] \
            * (
                dt.combine(end_date,t(0,0,0)) + td(days=1)
                - dt.combine(start_date,t(0,0,0)) \
            ).total_seconds() / 3600
        
        # Aggregate by resource id and nature-of-work:
        df = df.loc[:,['RESOURCE ID','NATURE OF WORK','APPLICABLE PMAX MWH','OUTAGE MWH DURING DEMAND']].groupby(['RESOURCE ID','NATURE OF WORK']).agg({
            'APPLICABLE PMAX MWH': 'max',
            'OUTAGE MWH DURING DEMAND': 'sum'
        }).reset_index()

        # Calculate forced outage rate for selected time range as outage MWh/
        # maximum possible MWh:
        df.loc[:,'EQUIVALENT FORCED OUTAGE RATE'] = df.loc[:,'OUTAGE MWH DURING DEMAND'] / df.loc[:,'APPLICABLE PMAX MWH']
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

        # Get prepared curtailment data:
        df = self.prepare_curtailment_data()

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
                hf
            )
        df.loc[:,'APPLICABLE OUTAGE HOURS'] = df.apply(
            f,
            axis='columns',
            result_type='expand'
        )

        # Calculate curtailed capacity * duration in MWh
        self.status_logger.log('\tCalculating outage rates within hour filter.')
        df.loc[:,'OUTAGE MWH DURING DEMAND'] = df.loc[:,'CURTAILMENT MW'] \
            * df.loc[:,'APPLICABLE OUTAGE HOURS']

        # Calculate maximum capacity * time range in MWh
        df.loc[:,'APPLICABLE PMAX MWH'] = df.loc[:,'RESOURCE PMAX MW'] \
            * (
                 hour_filter.loc[:,'END DATETIME'] \
                - hour_filter.loc[:,'START DATETIME']
            ).sum().total_seconds() / 3600
        
        # Aggregate by resource id and nature-of-work:
        df = df.loc[:,['RESOURCE ID','NATURE OF WORK','APPLICABLE PMAX MWH','OUTAGE MWH DURING DEMAND']].groupby(['RESOURCE ID','NATURE OF WORK']).agg({
            'APPLICABLE PMAX MWH': 'max',
            'OUTAGE MWH DURING DEMAND': 'sum'
        }).reset_index()

        # Calculate forced outage rate for selected time range as :
        df.loc[:,'EQUIVALENT FORCED OUTAGE RATE DURING DEMAND'] = df.loc[:,'OUTAGE MWH DURING DEMAND'] / df.loc[:,'APPLICABLE PMAX MWH']
        return df

    def calculate_equivalent_forced_outage_rate_during_shared_demand_hours(self,shared_hour_filter:list):
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

        # Get prepared curtailment data:
        df = self.prepare_curtailment_data()

        # Remove any curtailment reports without an end date time:
        df = df.loc[df.loc[:,'CURTAILMENT END DATE TIME'].notnull(),:]

        # Filter curtailment data for reports containing dates within input
        # range:
        self.status_logger.log('\tFiltering curtailment reports.',criticality='INFORMATION')
        start_datetime = shared_hour_filter.loc[:,'START DATETIME'].min().to_pydatetime()
        end_datetime = shared_hour_filter.loc[:,'END DATETIME'].max().to_pydatetime()
        
        # Split curtailment data into smaller chunks for multiprocessing:
        df_mp = [{
            'df': x,
            'start_datetime': start_datetime,
            'end_datetime': end_datetime,
            'natures_of_work': self.natures_of_work,
            'master_capability_list' : self.master_capability_list,
            'shared_hour_filter' : shared_hour_filter
        } for x in np.array_split(df,32)]

        # Run multiprocessing helper function on each chunk in parallel:
        with mp.Pool(processes=8) as mp_pool:
            df = pd.concat(mp_pool.map(multiprocessing_helper_function,df_mp))

        df.sort_values(by=['RESOURCE ID','NATURE OF WORK'],inplace=True)

        return df
    
    def prepare_curtailment_data(self):
        '''
        Filters and condenses curtailment reports from multiple prior trade-day
        curtailment report files to prevent double-counting and missing
        curtailment events. The following scenarios are considered:
            + A curtailment event lasts multiple days, and the end datetime is
              only reported on the final day--apply only the final version
            + A curtailment event reported in one prior trade-day report file is
              adjusted in a later prior trade-day report file--apply only the
              final version
            + A single MRID is used to report multiple outage events with
              varying curtailed capacities and start and end datetimes--use the
              final version of each time block.
            + No end datetime is reported for a given time block, but later time
              blocks are reported--use the earlier of either the start datetime
              of the soonest later time block or the end of the report day as
              the missing end datetime of the earlier time block.
        Overlapping outages with the same MRID and nature-of-work code are
        combined.
        '''

        # Copy curtailment data for analysis:
        df = self.curtailment_data.copy()

        # Use last version of each reported outage time block:
        df = df.groupby([
            'OUTAGE MRID',
            'RESOURCE ID',
            'OUTAGE TYPE',
            'NATURE OF WORK',
            'CURTAILMENT START DATE TIME'
        ]).last().reset_index()

        # Sort curtailment reports:
        df = df.sort_values(
            by=[
                'RESOURCE ID',
                'OUTAGE MRID',
                'REPORT DATE',
                'CURTAILMENT START DATE TIME'
            ]
        )

        # Create a sequential index column and previous index for join
        # operation:
        df.loc[:,'ROW NUMBER'] = df.reset_index().index
        df.loc[:,'PREVIOUS ROW NUMBER'] = df.loc[:,'ROW NUMBER'] - 1

        # Join each record with the next time block in the same outage event:
        df = df.set_index([
            'ROW NUMBER',
            'OUTAGE MRID',
            'RESOURCE ID',
            'OUTAGE TYPE',
            'NATURE OF WORK'
        ]).join(
            df.loc[
                :,
                [
                    'PREVIOUS ROW NUMBER',
                    'OUTAGE MRID',
                    'RESOURCE ID',
                    'OUTAGE TYPE',
                    'NATURE OF WORK',
                    'CURTAILMENT START DATE TIME',
                    'CURTAILMENT END DATE TIME'
                ]
            ].rename(
                columns={
                    'PREVIOUS ROW NUMBER' : 'ROW NUMBER',
                    'CURTAILMENT START DATE TIME' : 'NEXT CURTAILMENT START DATE TIME',
                    'CURTAILMENT END DATE TIME' : 'NEXT CURTAILMENT END DATE TIME'
                }
            ).set_index([
                'ROW NUMBER',
                'OUTAGE MRID',
                'RESOURCE ID',
                'OUTAGE TYPE',
                'NATURE OF WORK'
            ]),
            how='left'
        ).reset_index()

        # Replace missing end datetimes with the earlier of either the start of
        # the next report day or the start of the next time block with the same
        # MRID:
        df.loc[
            df.loc[:,'NEXT CURTAILMENT START DATE TIME'].isnull(),
            'NEXT CURTAILMENT START DATE TIME'
        ] = df.loc[
            df.loc[:,'NEXT CURTAILMENT START DATE TIME'].isnull(),
            'REPORT DATE'
        ].map(lambda x: dt.combine(x,t(0,0,0)) + td(days=1))
        df.loc[
            df.loc[:,'CURTAILMENT END DATE TIME'].isnull(),
            'CURTAILMENT END DATE TIME'
        ] = df.loc[df.loc[:,'CURTAILMENT END DATE TIME'].isnull(),:].apply(
            lambda r: min(
                r.loc['NEXT CURTAILMENT START DATE TIME'],
                dt.combine(r.loc['REPORT DATE'],t(0,0,0)) + td(days=1)
            ),
            axis='columns',
            result_type='expand'
        )

        # Drop extra columns:
        df = df.reset_index().drop(
            columns=[
                'index',
                'ROW NUMBER',
                'PREVIOUS ROW NUMBER',
                'NEXT CURTAILMENT START DATE TIME',
                'NEXT CURTAILMENT END DATE TIME'
            ]
        )

        # Return results:
        return df

### Multiprocessing Helper Functions: ###
def multiprocessing_helper_function(chunk):
    '''
    A helper function to be called by a multiprocessing object's map() method,
    along with chunked data passed individually as the chunk input.
    '''

    # Filter curtailment data for reports within date range of the demand hours:
    df = chunk['df']
    df.loc[:,'INCLUDE'] = df.apply(
        lambda r: datetime_range_overlap(
                    chunk['start_datetime'],
                    chunk['end_datetime'],
                    r.loc['CURTAILMENT START DATE TIME'],
                    r.loc['CURTAILMENT END DATE TIME']
        )>0,
        axis='columns',
        result_type='expand'
    )
    df = df.loc[df.loc[:,'INCLUDE'],:].sort_values(by=['RESOURCE ID','OUTAGE MRID','NATURE OF WORK','CURTAILMENT START DATE TIME'])

    # Filter curtailment data for forced outages and nature-of-work codes
    # listed in the configuration file:
    df = df.loc[(df.loc[:,'OUTAGE TYPE']=='FORCED'),:]
    if chunk['natures_of_work'] is not None:
        df.loc[:,'INCLUDE'] = df.apply(
            lambda r: r.loc['NATURE OF WORK'] in chunk['natures_of_work'],
            axis='columns',
            result_type='expand'
        )
        df = df.loc[df.loc[:,'INCLUDE'],:]

    # Filter curtailment data using commercial operation start date in the
    # Master Capability List:
    df = df.set_index('RESOURCE ID').join(
        chunk['master_capability_list'].loc[:,['ResID','CommercialOperDate']].set_index('ResID')
    ).reset_index().rename(
        columns={
            'index':'RESOURCE ID',
            'CommercialOperDate':'COMMERCIAL OPERATION DATE'
        }
    )
    df.loc[df.loc[:,'COMMERCIAL OPERATION DATE'].isnull(),'COMMERCIAL OPERATION DATE'] = dt(1900,1,1,0,0)
    df.loc[(df.loc[:,'CURTAILMENT END DATE TIME']>=df.loc[:,'COMMERCIAL OPERATION DATE']),:]

    # Constrain the starts of curtailments to commercial operation date:
    df.loc[:,'CURTAILMENT START DATE TIME'] = df.apply(
        lambda r: max(r.loc[['CURTAILMENT START DATE TIME','COMMERCIAL OPERATION DATE']]),
        axis='columns',
        result_type='expand'
    )

    # Calculate reported outage hours within date range and hour filter:
    df = df.loc[
        (df.loc[:,'CURTAILMENT START DATE TIME']<=chunk['shared_hour_filter'].loc[:,'END DATETIME'].max()) \
        & (df.loc[:,'CURTAILMENT END DATE TIME']>=chunk['shared_hour_filter'].loc[:,'START DATETIME'].min()),
        :
    ]

    # Calculate overlap between outage and demand hours:
    df.loc[:,'APPLICABLE OUTAGE HOURS'] = df.apply(
        lambda r:hour_filter_overlap(
            r.loc['CURTAILMENT START DATE TIME'],
            r.loc['CURTAILMENT END DATE TIME'],
            chunk['shared_hour_filter']
        ),
        axis='columns',
        result_type='expand'
    )

    # Calculate curtailed capacity * outage duration during demand in MWh
    df.loc[:,'OUTAGE MWH DURING DEMAND'] = df.loc[:,'CURTAILMENT MW'] \
        * df.loc[:,'APPLICABLE OUTAGE HOURS']

    # Aggregate by resource id and nature-of-work:
    df = df.loc[:,[
        'RESOURCE ID','NATURE OF WORK','OUTAGE MWH DURING DEMAND'
    ]].groupby(['RESOURCE ID','NATURE OF WORK']).agg({
        'OUTAGE MWH DURING DEMAND': 'sum'
    }).reset_index()

    return df