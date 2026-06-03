import random
import numpy as np
import pandas as pd
import multiprocessing as mp
from pathlib import Path
from datetime import datetime as dt,timedelta as td

from src.logging.logging import TextLogger
from src.utils.string_functions import replace_template_placeholders
from src.utils.prepare_curtailment_data import prepare_curtailment_data
from src.outage_rate_evaluator.outage_rate_evaluator import OutageRateEvaluator

class UCAPEvaluator:
    '''
    A class to calculate unforced capacities for applicable resources based on
    Equivalent Forced Outage Rates during Demand hours (EFORd) calculated using
    outages and curtailments with nature-of-work codes specified in the
    config.yaml file, and weather-normalized EFORd values for derations due to
    ambient temperatures. Additional settings in the config.yaml file are as
    follows:
        ucap_analysis.historic_evaluation_period - Specifies the integer number
            of complete calendar years of historic outage and weather data to
            use when evaluating UCAP
        ucap_analysis.year_exclusion_count - Specifies the integer number of
            years of historic outages to exclude when evaluating EFORd based
            based on all natures-of-work except derations due to ambient
            temperatures
        ucap_analysis.excluded_natures_of_work - Specifies which nature-of-work
            codes to ignore when evaluating UCAP
        ucap_analysis.seasons - Defines seasons for the purpose of calculating
            and applying UCAP values for individual resources
        ucap_analysis.resource_types.ucap_eligible - Specifies the set of
            resource types, as defined in the Master Resource Database, for
            which UCAP values should be calculated and applied
        ucap_analysis.resource_types.weather_normalization - Specifies the set
            of resource types for which derations due to ambient temperatures
            are to be weather-normalized


    This class replicates the functionality of the UCAP evaluation Excel
    workbook.
    '''

    def __init__(self,config:dict):
        self.status_logger = TextLogger(
            cli_logging_criticalities=['INFORMATION','WARNING','ERROR'],
            file_logging_criticalities=['WARNING','ERROR'],
            log_path=config['ucap_analysis']['text_log_path']
        )
        self.outage_rate_evaluator = OutageRateEvaluator(config)
        self.master_resource_database_path = Path(config['resource_information']['master_resource_database']['path'])
        self.master_resource_database_worksheet_name = config['resource_information']['master_resource_database']['worksheet_name']
        self.caiso_master_capability_list_worksheet_name = config['resource_information']['master_resource_database']['caiso_master_capability_list_worksheet_name']
        self.excluded_natures_of_work = config['ucap_analysis']['excluded_natures_of_work']
        self.seasons = config['ucap_analysis']['seasons']
        self.ucap_resource_types = config['ucap_analysis']['resource_types']['ucap_eligible']
        self.weather_normalized_resource_types = config['ucap_analysis']['resource_types']['weather_normalization']
        self.outage_rates_path_template = config['ucap_analysis']['results']['outage_rates_path_template']
        self.normalized_deration_rates_path_template = config['ucap_analysis']['results']['ambient_derations_due_to_temperature']['normalized_deration_rates_path_template']
        self.time_to_fail_and_time_to_repair_distributions_path_template = config['ucap_analysis']['results']['servm_inputs']['time_to_fail_and_time_to_repair_distributions_path_template']
        self.maintenance_outage_rate_distributions_path_template = config['ucap_analysis']['results']['servm_inputs']['maintenance_outage_rate_distributions_path_template']
        self.years = config['ucap_analysis']['years']
        self.seasons = config['ucap_analysis']['seasons']
        self.year_exclusion_count = config['ucap_analysis']['year_exclusion_count']
        self.hour_filter_path = Path(config['ucap_analysis']['hour_filter_path'])

        self.demand_hours = pd.DataFrame()
        self.master_resource_database = pd.DataFrame()
        self.caiso_master_capability_list = pd.DataFrame()
        self.outage_rates = pd.DataFrame()
        self.normalized_deration_rates = pd.DataFrame()
        self.time_to_fail_and_time_to_repair_distributions = pd.DataFrame()
        self.maintenance_outage_rate_distributions = pd.DataFrame()

        self.ucap_by_resource_season_path_template = config['ucap_analysis']['results']['ucap_by_resource_season_path_template']
        self.ucap_by_resource_type_season_path_template = config['ucap_analysis']['results']['ucap_by_resource_type_season_path_template']

        self.mp_processes_count = config['multiprocessing']['processes_count']

    def get_outage_rates(self):
        '''
        Reads the .csv file containing evaluated EFORd values and loads the
        contents into a dataframe for further analysis.

            Returns: a copy of the EFORd dataframe
            Side Effects: stores a copy of the EFORd dataframe to the
                outage_rates object parameter
        '''
        if self.outage_rates.empty:
            outage_rates_path = Path(replace_template_placeholders(self.outage_rates_path_template,{'years' : f'{self.years[0]}-{self.years[-1]}'}))

            df = pd.read_csv(outage_rates_path)

            df['RESOURCE ID'] = df['RESOURCE ID'].map(lambda s:s.replace(' ','_'))

            self.status_logger.log('Loaded Outage Rates')
            self.outage_rates = df.copy()
        else:
            df = self.outage_rates.copy()

        return df

    def get_normalized_deration_rates(self):
        '''
        Reads the .csv file containing evaluated weather-normalized EFORd values
        based on ambient derations due to temperature and loads the contents
        into a dataframe for further analysis.

            Returns: a copy of the weather-normalized EFORd dataframe
            Side Effects: stores a copy of the weather-normalized EFORd
                dataframe to the outage_rates object parameter
        '''

        if self.normalized_deration_rates.empty:
            normalized_deration_rates_path = Path(replace_template_placeholders(self.normalized_deration_rates_path_template,{'years' : f'{self.years[0]}-{self.years[-1]}'}))
            df = pd.read_csv(normalized_deration_rates_path)
            df['RESOURCE ID'] = df['RESOURCE ID'].map(lambda s:s.replace(' ','_'))

            self.status_logger.log('Loaded Weather-Normalized Deration Rates due to Ambient Temperatures','INFORMATION')
            self.normalized_deration_rates = df.copy()
        else:
            df = self.normalized_deration_rates.copy()

        return df

    def get_master_resource_database(self):
        if self.master_resource_database.empty:
            df = pd.read_excel(self.master_resource_database_path,self.master_resource_database_worksheet_name)
            df['Resource ID'] = df['Resource ID'].map(lambda s:s.replace(' ','_'))

            self.status_logger.log('Loaded Master Resource Database from Excel file','INFORMATION')
            self.master_resource_database = df.copy()
        else:
            df = self.master_resource_database.copy()
        return df

    def get_caiso_master_capability_list(self):
        if self.caiso_master_capability_list.empty:
            df = pd.read_excel(self.master_resource_database_path,self.caiso_master_capability_list_worksheet_name)
            df['RESOURCE_ID'] = df['RESOURCE_ID'].map(lambda s:s.replace(' ','_'))

            df['COD'] = df['COD'].map(lambda x:dt.fromordinal(x+42137) if type(x)==int else x)

            self.status_logger.log('Loaded CAISO Master Capability List from Excel file','INFORMATION')
            self.caiso_master_capability_list = df.copy()
        else:
            df = self.caiso_master_capability_list.copy()
        return df

    def get_demand_hours(self):
        if self.demand_hours.empty:
            df = pd.read_csv(self.hour_filter_path)
            df['START DATETIME'] = df['START DATETIME'].astype('datetime64')
            df['END DATETIME'] = df['END DATETIME'].astype('datetime64')
            df['SEASON'] = df['SEASON'].astype('string')
            self.status_logger.log('Loaded Demand Hours from CSV','INFORMATION')
            self.demand_hours = df.copy()
        else:
            df = self.demand_hours.copy()
        return df

    def get_season(self,t:dt):
        seasons = {
            k:[[dt.strptime(x[0]+' '+str(t.year),r'%b %d %Y'),dt.strptime(x[1]+' '+str(t.year),r'%b %d %Y')] for x in v]
            for k,v in self.seasons.items()
        }
        for season_name,date_ranges in self.seasons.items():
            for date_range in date_ranges:
                start_date = dt.strptime(date_range[0]+' '+str(t.year),r'%b %d %Y')
                end_date = dt.strptime(date_range[1]+' '+str(t.year),r'%b %d %Y')+td(days=1)
                if t>=start_date and t<end_date:
                    return season_name
                else:
                    pass
        return None

    def get_time_to_fail_and_time_to_repair_distributions(self):
        '''
        Calculates or loads from memory the time-to-fail and time-to-repair
        distributions for each UCAP-eligible resource.
        '''
        if self.time_to_fail_and_time_to_repair_distributions.empty:
            df = self.outage_rate_evaluator.calculate_time_to_fail_and_time_to_repair_distributions('FORCED','PLANT_TROUBLE')
            self.time_to_fail_and_time_to_repair_distributions = df.copy()
        else:
            df = self.time_to_fail_and_time_to_repair_distributions.copy()
        return df

    def get_maintenance_outage_rate_distributions(self):
        '''
        Calculates or loads from memory the maintenance outage rates for each
        UCAP-eligible resource
        '''
        if self.maintenance_outage_rate_distributions.empty:
            df = self.outage_rate_evaluator.calculate_time_to_fail_and_time_to_repair_distributions('PLANNED','PLANT_MAINTENANCE')
            self.maintenance_outage_rate_distributions = df.copy()
        else:
            df = self.maintenance_outage_rate_distributions.copy()
        return df

    def evaluate_ucap(self):
        '''
        Calculates individual UCAP values for each applicable resource.
        '''

        # Determine save paths:
        ucap_by_resource_season_path = replace_template_placeholders(self.ucap_by_resource_season_path_template,{'years' : f'{self.years[0]}-{self.years[-1]}'})
        ucap_by_resource_type_season_path = replace_template_placeholders(self.ucap_by_resource_type_season_path_template,{'years' : f'{self.years[0]}-{self.years[-1]}'})

        # Load data from files:
        master_resource_database = self.get_master_resource_database()
        caiso_master_capability_list = self.get_caiso_master_capability_list()
        outage_rates = self.get_outage_rates()
        normalized_deration_rates = self.get_normalized_deration_rates()
        demand_hours = self.get_demand_hours()

        demand_hours['YEAR'] = demand_hours['START DATETIME'].dt.year
        demand_hours['UCAP SEASON'] = demand_hours['START DATETIME'].map(self.get_season)
        demand_hours = demand_hours.loc[demand_hours['DEMAND HOUR'],:]
        demand_hours = demand_hours.set_index(['YEAR','UCAP SEASON'])

        # Prepare tables of all resources, resource types, years, and seasons:
        resources = master_resource_database.loc[
            (master_resource_database['Resource Type'].map(lambda s: s in self.ucap_resource_types)) &
            (
                (master_resource_database['Dispatchability']=='Y') |
                (master_resource_database['Resource Type']=='Nuclear')
            ),
            ['Resource ID','Resource Type','Pmax/NDC']
        ]
        resources['Resource ID'] = resources['Resource ID'].astype('string')
        resources['Resource Type'] = resources['Resource Type'].astype('string')
        resources['Pmax/NDC'] = resources['Pmax/NDC'].astype('float64')
        resource_types = pd.DataFrame({'Resource Type':self.ucap_resource_types},dtype='string')
        years = pd.DataFrame({'Year':self.years},dtype='int64')
        seasons = pd.DataFrame({'Season':self.seasons.keys()},dtype='string')

        # Setup a dataframe to store aggregations by resource, year, and season:
        r_y_s = resources.join(years,how='cross').join(seasons,how='cross')

        # Setup a dataframe to store aggregations by resource and year:
        r_y = resources.join(years,how='cross')

        # Setup a dataframe to store aggregations by resource type, year, and
        # season:
        rt_y_s = resource_types.join(years,how='cross').join(seasons,how='cross')

        # Setup a dataframe to store aggregations by resource type and season:
        rt_s = resource_types.merge(
            resources[['Resource Type','Pmax/NDC']].groupby('Resource Type').sum().reset_index(),
            on='Resource Type',
            how='inner'
        ).join(
            seasons,
            how='cross'
        )

        # Setup a dataframe to store aggregations by resource and season, to
        # contain the final results
        r_s = resources.join(seasons,how='cross')

        # Calculate initial aggregations by resource, year, and season (parallelized):
        r_y_s = r_y_s.merge(
            caiso_master_capability_list[['RESOURCE_ID','COD']],
            left_on='Resource ID',right_on='RESOURCE_ID',
            how='left'
        )
        mp_chunks = [{
            'df' : r_y_s.loc[(r_y_s['Year']==r['Year'])*(r_y_s['Season']==r['Season']),:].copy(),
            'outage_rates' : outage_rates.loc[(outage_rates['YEAR']==r['Year'])*(outage_rates['SEASON']==r['Season']),:],
            'normalized_deration_rates' : normalized_deration_rates.loc[(normalized_deration_rates['SEASON']==r['Season']),:],
            'demand_hours' : demand_hours.loc[(r['Year'],r['Season']),:].reset_index(),
            'excluded_natures_of_work' : self.excluded_natures_of_work
        } for _,r in years.merge(seasons,how='cross').iterrows()]
        with mp.Pool(processes=self.mp_processes_count) as mp_pool:
            r_y_s = pd.concat(mp_pool.map(first_resource_aggregations_by_resource_year_and_season,mp_chunks))
        r_y_s.sort_values(by=['Resource ID','Year','Season'],inplace=True)

        # Calculate initial aggregations by resource and year:
        r_y = r_y.merge(
            r_y_s[[
                'Resource ID',
                'Year',
                'Individual Calendar Year Demand Hours',
                'Group Calendar Year Demand Hours',
                'Individual Typical Weather Year Demand Hours',
                'Group Typical Weather Year Demand Hours',
                'Outage MWh during Demand Excluding Ambient',
                'Weather-Normalized Deration MWh during Demand',
                'Individual MWh at Pmax during Calendar Year Demand',
                'Group MWh at Pmax during Calendar Year Demand',
                'Individual MWh at Pmax during Typical Weather Year Demand',
                'Group MWh at Pmax during Typical Weather Year Demand'
            ]].groupby(['Resource ID','Year']).sum().reset_index(),
            on=['Resource ID','Year'],
            how='inner'
        )
        r_y['Individual EFORd Excluding Ambient'] = r_y['Outage MWh during Demand Excluding Ambient'] \
            / r_y_s['Individual MWh at Pmax during Calendar Year Demand']
        r_y['Individual EFORd Weather-Normalized Ambient'] = r_y['Weather-Normalized Deration MWh during Demand'] \
            / r_y_s['Individual MWh at Pmax during Typical Weather Year Demand']

        # Calculate initial aggregations by resource type, year, and season:
        rt_y_s = rt_y_s.merge(
            r_y_s[[
                'Resource Type',
                'Year',
                'Season',
                'Outage MWh during Demand Excluding Ambient',
                'Weather-Normalized Deration MWh during Demand',
                'Individual MWh at Pmax during Calendar Year Demand',
                'Individual MWh at Pmax during Typical Weather Year Demand',
            ]].groupby(['Resource Type','Year','Season']).sum().reset_index(),
            on=['Resource Type','Year','Season'],
            how='inner'
        )
        rt_y_s['EFORd Excluding Ambient First Pass'] = rt_y_s['Outage MWh during Demand Excluding Ambient'] \
            / rt_y_s['Individual MWh at Pmax during Calendar Year Demand']
        rt_y_s['EFORD Weather-Normalized Ambient First Pass'] = rt_y_s['Weather-Normalized Deration MWh during Demand'] \
            / rt_y_s['Individual MWh at Pmax during Typical Weather Year Demand']

        # Calculate second aggregations by resource and year:
        def f(r):
            df = rt_y_s.loc[
                (rt_y_s['Resource Type']==r['Resource Type'])
                * (rt_y_s['Year']==r['Year']),
                [
                    'Outage MWh during Demand Excluding Ambient',
                    'Individual MWh at Pmax during Calendar Year Demand',
                    'Weather-Normalized Deration MWh during Demand',
                    'Individual MWh at Pmax during Typical Weather Year Demand'
                ]
            ]
            return pd.Series((
                df['Outage MWh during Demand Excluding Ambient'].dot(df['Individual MWh at Pmax during Calendar Year Demand']),
                df['Weather-Normalized Deration MWh during Demand'].dot(df['Individual MWh at Pmax during Typical Weather Year Demand'])
            ))
        r_y[['Group EFORd Excluding Ambient','Group EFORd Weather-Normalized Ambient']] = r_y.apply(
            f,
            axis='columns',
            result_type='expand'
        )
        def f(r):
            if r['Individual Calendar Year Demand Hours']>0 and r['Group Calendar Year Demand Hours']>0:
                a = (
                    r['Individual Calendar Year Demand Hours'] * r['Individual EFORd Excluding Ambient']
                    + r['Group Calendar Year Demand Hours'] * r['Group EFORd Excluding Ambient']
                ) / (
                     r['Individual Calendar Year Demand Hours']
                     + r['Group Calendar Year Demand Hours']
                )
                b = (
                    r['Individual Typical Weather Year Demand Hours'] * r['Individual EFORd Weather-Normalized Ambient']
                    + r['Group Typical Weather Year Demand Hours'] * r['Group EFORd Weather-Normalized Ambient']
                ) / (
                     r['Individual Typical Weather Year Demand Hours']
                     + r['Group Typical Weather Year Demand Hours']
                )
            elif r['Individual Calendar Year Demand Hours']>0:
                a = r['Individual EFORd Excluding Ambient']
                b = r['Individual EFORd Weather-Normalized Ambient']
            elif r['Group Calendar Year Demand Hours']>0:
                a = r['Group EFORd Excluding Ambient']
                b = r['Group EFORd Weather-Normalized Ambient']
            else:
                a = 0.0
                b = 0.0
            return pd.Series((a, b, a + b))
        r_y[['EFORd Exluding Ambient','EFORd Weather-Normalized Ambient','EFORd']] = r_y.apply(
            f,
            axis='columns',
            result_type='expand'
        )
        r_y['Resource Year Rank'] = r_y[['Resource ID','EFORd']].groupby('Resource ID').rank()
        r_y['Include Resource Year in UCAP'] = r_y['Resource Year Rank'] <= len(self.years) - self.year_exclusion_count

        # Calculate second aggregations by Resource, Year, and Season (parallelized):
        r_y_s = r_y_s.merge(
            r_y[['Resource ID','Year','Include Resource Year in UCAP']],
            on=['Resource ID','Year'],
            how='inner'
        )
        mp_chunks = [{
            'df' : r_y_s.loc[(r_y_s['Year']==r['Year'])*(r_y_s['Season']==r['Season']),:].copy(),
            'demand_hours' : demand_hours.loc[(r['Year'],r['Season']),:].reset_index()
        } for _,r in years.join(seasons,how='cross').iterrows()]
        with mp.Pool(processes=self.mp_processes_count) as mp_pool:
            r_y_s = pd.concat(mp_pool.map(second_resource_aggregations_by_resource_year_and_season,mp_chunks))
        r_y_s.sort_values(by=['Resource ID','Year','Season'],inplace=True)

        # Calculate aggregations by resource and season:
        r_s = r_s.merge(
            r_y_s.loc[(r_y_s['Include Resource Year in UCAP']),[
                'Resource ID',
                'Season',
                'Individual Calendar Year Demand Hours',
                'Group Calendar Year Demand Hours',
                'Individual Typical Weather Year Demand Hours',
                'Group Typical Weather Year Demand Hours',
                'Outage MWh during Demand Excluding Ambient',
                'Group Outage MWh during Demand Excluding Ambient',
                'Weather-Normalized Deration MWh during Demand',
                'Group Weather-Normalized Deration MWh during Demand',
                'Individual MWh at Pmax during Calendar Year Demand',
                'Individual MWh at Pmax during Typical Weather Year Demand',
                'Group MWh at Pmax during Calendar Year Demand',
                'Group MWh at Pmax during Typical Weather Year Demand'
            ]].groupby(['Resource ID','Season']).sum().reset_index(),
            on=['Resource ID','Season'],
            how='inner'
        )
        r_s['Individual EFORd Excluding Ambient'] = r_s['Outage MWh during Demand Excluding Ambient'] \
            / r_s['Individual MWh at Pmax during Calendar Year Demand']
        r_s['Individual EFORd Weather-Normalized Ambient'] = r_s['Weather-Normalized Deration MWh during Demand'] \
            / r_s['Individual MWh at Pmax during Typical Weather Year Demand']
        r_s['Group EFORd Excluding Ambient'] = r_s['Group Outage MWh during Demand Excluding Ambient'] \
            / r_s['Group MWh at Pmax during Calendar Year Demand']
        r_s['Group EFORd Weather-Normalized Ambient'] = r_s['Group Weather-Normalized Deration MWh during Demand'] \
            / r_s['Group MWh at Pmax during Typical Weather Year Demand']
        r_s.fillna(value=0,inplace=True)
        r_s['EFORd Excluding Ambient'] = (
            r_s['Individual Calendar Year Demand Hours'] * r_s['Individual EFORd Excluding Ambient']
            + r_s['Group Calendar Year Demand Hours'] * r_s['Group EFORd Excluding Ambient']
        )/(
            r_s['Individual Calendar Year Demand Hours'] + r_s['Group Calendar Year Demand Hours']
        )
        r_s['EFORd Weather-Normalized Ambient'] = (
            r_s['Individual Typical Weather Year Demand Hours'] * r_s['Individual EFORd Weather-Normalized Ambient']
            + r_s['Group Typical Weather Year Demand Hours'] * r_s['Group EFORd Weather-Normalized Ambient']
        )/(
            r_s['Individual Typical Weather Year Demand Hours'] + r_s['Group Typical Weather Year Demand Hours']
        )
        r_s['EFORd'] = r_s['EFORd Excluding Ambient'] + r_s['EFORd Weather-Normalized Ambient']
        r_s['UCAP MW'] = r_s['Pmax/NDC'] * (1 - r_s['EFORd'])
        r_s = r_s.sort_values(by=['Resource Type','Resource ID','Season'])

        # Calculate final aggregation by resource type and season:
        rt_s = rt_s.merge(
            r_y_s.loc[(r_y_s['Include Resource Year in UCAP']),[
                'Resource Type',
                'Season',
                'Individual Calendar Year Demand Hours',
                'Outage MWh during Demand Excluding Ambient',
                'Weather-Normalized Deration MWh during Demand',
                'Individual MWh at Pmax during Calendar Year Demand',
                'Individual MWh at Pmax during Typical Weather Year Demand'
            ]].groupby(['Resource Type','Season']).sum().reset_index(),
            on=['Resource Type','Season'],
            how='inner'
        )
        rt_s['EFORd Excluding Ambient'] = rt_s['Outage MWh during Demand Excluding Ambient'] \
            / rt_s['Individual MWh at Pmax during Calendar Year Demand']
        rt_s['EFORd Weather-Normalized Ambient'] = rt_s['Weather-Normalized Deration MWh during Demand'] \
            / rt_s['Individual MWh at Pmax during Typical Weather Year Demand']
        rt_s['EFORd'] = rt_s['EFORd Excluding Ambient'] + rt_s['EFORd Weather-Normalized Ambient']
        rt_s['UCAP MW'] = rt_s['Pmax/NDC'] * (1 - rt_s['EFORd'])
        rt_s = rt_s.sort_values(by=['Resource Type','Season'])

        # Save results to file:
        r_s.to_csv(ucap_by_resource_season_path,index=False)
        rt_s.to_csv(ucap_by_resource_type_season_path,index=False)
        return (r_s,rt_s)

    def generate_servm_outage_statistics_files(self):
        '''
        Calculates or retrieves time-to-fail and time-to-repair distributions
        for all UCAP resources, based separately on forced plant trouble and
        planned plant maintenanceand saves the results in a format useable in
        SERVM
        '''

        # Plant trouble outage distributions:
        time_to_fail_and_time_to_repair_distributions_path = (replace_template_placeholders(
            self.time_to_fail_and_time_to_repair_distributions_path_template,
            {'years' : '{}-{}'.format(min(self.years),max(self.years))}
        ))
        df = self.get_time_to_fail_and_time_to_repair_distributions()
        df.to_csv(time_to_fail_and_time_to_repair_distributions_path,index=False)

        # Maintenance outage distributions:
        maintenance_outage_rate_distributions_path = Path(replace_template_placeholders(
            self.maintenance_outage_rate_distributions_path_template,
            {'years' : '{}-{}'.format(min(self.years),max(self.years))}
        ))
        df = self.get_maintenance_outage_rate_distributions()
        df.to_csv(maintenance_outage_rate_distributions_path,index=False)

def first_resource_aggregations_by_resource_year_and_season(chunk):
    '''
    Helper function for parallelizing calculations in first aggregation by
    resource, year, and season
    '''
    df = chunk['df']
    outage_rates = chunk['outage_rates']
    normalized_deration_rates = chunk['normalized_deration_rates']
    demand_hours = chunk['demand_hours']
    excluded_natures_of_work = chunk['excluded_natures_of_work']

    df['Outage MWh during Demand Excluding Ambient'] = df.apply(
        lambda r:outage_rates.loc[
            (outage_rates['RESOURCE ID']==r['Resource ID'])
            *(outage_rates['NATURE OF WORK']!='AMBIENT_DUE_TO_TEMP')
            *(outage_rates['NATURE OF WORK'].map(lambda s: s not in excluded_natures_of_work)),
            'OUTAGE MWH DURING DEMAND'
        ].sum(),
        axis='columns',
        result_type='expand'
    )
    df['Weather-Normalized Deration MWh during Demand'] = df.apply(
        lambda r:normalized_deration_rates.loc[
            (normalized_deration_rates['RESOURCE ID']==r['Resource ID'])
            *(normalized_deration_rates['NATURE OF WORK']=='AMBIENT_DUE_TO_TEMP'),
            'OUTAGE MWH DURING DEMAND'
        ].sum(),
        axis='columns',
        result_type='expand'
    )
    def f(r):
        return (
            demand_hours.loc[(demand_hours['START DATETIME']>=r['COD']),'DEMAND HOUR'].count(),
            demand_hours.loc[(demand_hours['START DATETIME']<r['COD']),'DEMAND HOUR'].count()
        )
    df[
        [
            'Individual Calendar Year Demand Hours',
            'Group Calendar Year Demand Hours'
        ]
    ] = df.apply(
        f,
        axis='columns',
        result_type='expand'
    )
    def f(r):
        return (
            demand_hours.loc[
                (demand_hours['START DATETIME']>=r['COD'])
                * (~((demand_hours['START DATETIME'].dt.month==2)*(demand_hours['START DATETIME'].dt.day==29))),
                'DEMAND HOUR'
            ].count(),
            demand_hours.loc[
                (demand_hours['START DATETIME']<r['COD'])
                * (~((demand_hours['START DATETIME'].dt.month==2)*(demand_hours['START DATETIME'].dt.day==29))),
                'DEMAND HOUR'
            ].count()
        )
    df[
        [
            'Individual Typical Weather Year Demand Hours',
            'Group Typical Weather Year Demand Hours'
        ]
    ] = df.apply(
        f,
        axis='columns',
        result_type='expand'
    )
    df['Individual MWh at Pmax during Calendar Year Demand'] = df['Pmax/NDC'] \
        * df['Individual Calendar Year Demand Hours']
    df['Individual MWh at Pmax during Typical Weather Year Demand'] = df['Pmax/NDC'] \
        * df['Individual Typical Weather Year Demand Hours']
    def f(r):
        if r['Individual Calendar Year Demand Hours']>0 and r['Group Calendar Year Demand Hours']:
            s = 'Blended'
        elif r['Individual Calendar Year Demand Hours']>0:
            s = 'Individual'
        elif r['Group Calendar Year Demand Hours']>0:
            s = 'Group'
        else:
            s = 'No Data'
        return s
    df['EFORd Assessment'] = df.apply(
        f,
        axis='columns',
        result_type='expand'
    )
    def f(r):
        if r['Group Calendar Year Demand Hours']>0:
            df1 = df.loc[
                (df['Resource ID']!=r['Resource ID'])
                *(df['Resource Type']==r['Resource Type']),
                :
            ]
            if not df1.empty:
                def g(q):
                    return (
                        q['Pmax/NDC'] * demand_hours.loc[
                            (demand_hours['START DATETIME']>=q['COD'])
                            *(demand_hours['START DATETIME']<r['COD']),
                            'DEMAND HOUR'
                        ].count(),
                        q['Pmax/NDC'] * demand_hours.loc[
                            (demand_hours['START DATETIME']>=q['COD'])
                            *(demand_hours['START DATETIME']<r['COD'])
                            *(~((demand_hours['START DATETIME'].dt.month==2)*(demand_hours['START DATETIME'].dt.day==29))),
                            'DEMAND HOUR'
                        ].count()
                    )

                return df1.apply(
                    g,
                    axis='columns',
                    result_type='expand'
                ).sum()
            else:
                return pd.Series((0.0,0.0))
        else:
            return pd.Series((0.0,0.0))
    df[[
        'Group MWh at Pmax during Calendar Year Demand',
        'Group MWh at Pmax during Typical Weather Year Demand'
    ]] = df.apply(
        f,
        axis='columns',
        result_type='expand'
    )
    df['Individual EFORd Excluding Ambient'] = df['Outage MWh during Demand Excluding Ambient'] \
        / df['Individual MWh at Pmax during Calendar Year Demand']
    df['Individual EFORd Weather-Normalized Ambient'] = df['Weather-Normalized Deration MWh during Demand'] \
        / df['Individual MWh at Pmax during Typical Weather Year Demand']
    df.fillna(value=0,inplace=True)
    return df

def second_resource_aggregations_by_resource_year_and_season(chunk):
    '''
    Helper function for parallelizing calculations in second aggregation by
    resource, year, and season
    '''
    df = chunk['df']
    demand_hours = chunk['demand_hours']
    def f(r):
        if r['Group Calendar Year Demand Hours']>0:
            df1 = df.loc[
                (df['Resource ID']!=r['Resource ID'])
                *(df['Resource Type']==r['Resource Type'])
                *(df['Include Resource Year in UCAP']),
                :
            ]
            if not df1.empty:
                def g(q):
                    return pd.Series((
                        q['Pmax/NDC'] * demand_hours.loc[
                            (demand_hours['START DATETIME']>=q['COD'])
                            *(demand_hours['START DATETIME']<r['COD']),
                            'DEMAND HOUR'
                        ].count(),
                        q['Pmax/NDC'] * demand_hours.loc[
                            (demand_hours['START DATETIME']>=q['COD'])
                            *(demand_hours['START DATETIME']<r['COD'])
                            *(~((demand_hours['START DATETIME'].dt.month==2)*(demand_hours['START DATETIME'].dt.day==29))),
                            'DEMAND HOUR'
                        ].count()
                    ))
                return pd.concat(
                    (
                        pd.Series((
                            df1['Outage MWh during Demand Excluding Ambient'].sum(),
                            df1['Weather-Normalized Deration MWh during Demand'].sum()
                        )),
                        df1.apply(
                            g,
                            axis='columns',
                            result_type='expand'
                        ).sum()
                    ),
                    ignore_index=True
                )
            else:
                return pd.Series((0.0,0.0,0.0,0.0))
        else:
            return pd.Series((0.0,0.0,0.0,0.0))
    df[[
        'Group Outage MWh during Demand Excluding Ambient',
        'Group Weather-Normalized Deration MWh during Demand',
        'Group MWh at Pmax during Calendar Year Demand',
        'Group MWh at Pmax during Typical Weather Year Demand'
    ]] = df.apply(
        f,
        axis='columns',
        result_type='expand'
    )
    df['Group EFORd Excluding Ambient'] = df['Group Outage MWh during Demand Excluding Ambient'] \
        / df['Group MWh at Pmax during Calendar Year Demand']
    df['Group EFORd Weather-Normalized Ambient'] = df['Group Weather-Normalized Deration MWh during Demand'] \
        / df['Group MWh at Pmax during Typical Weather Year Demand']
    df['Group EFORd'] = df['Group EFORd Excluding Ambient'] \
        + df['Group EFORd Weather-Normalized Ambient']
    df.fillna(value=0,inplace=True)
    df['EFORd Excluding Ambient'] = (
        df['Individual Calendar Year Demand Hours'] * df['Individual EFORd Excluding Ambient']
        + df['Group Calendar Year Demand Hours'] * df['Group EFORd Excluding Ambient']
    ) / (
            df['Individual Calendar Year Demand Hours']
            + df['Group Calendar Year Demand Hours']
    )
    df['EFORd Weather-Normalized Ambient'] = (
        df['Individual Typical Weather Year Demand Hours'] * df['Individual EFORd Weather-Normalized Ambient']
        + df['Group Typical Weather Year Demand Hours'] * df['Group EFORd Weather-Normalized Ambient']
    ) / (
            df['Individual Typical Weather Year Demand Hours']
            + df['Group Typical Weather Year Demand Hours']
    )
    return df