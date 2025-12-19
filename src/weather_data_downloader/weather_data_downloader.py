import io
import re
import pycurl
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime as dt,timedelta as td

from src.logging.logging import DataLogger,TextLogger
from src.utils.string_functions import replace_template_placeholders
from src.utils.geospatial_functions import geodesic_distance

class WeatherDataDownloader:
    '''
    A class to manage downloads of NCEI/NOAA hourly global surface temperatures.
    '''
    download_directory_path = Path()
    weather_stations = []
    years = []
    logger = None
    weather_station_information = pd.DataFrame
    normalized_weather_inventory = pd.DataFrame
    def __init__(self,config:dict):
        log_dtypes = {
            'effective_date' : 'datetime64[D]',
            'usaf' : 'string',
            'wban' : 'string',
            'callsign' : 'string',
            'type' : 'string',
            'source_url' : 'string',
            'download_path' : 'string',
            'loaded_to_parquet' : 'int64',
        }

        self.historic_weather_data_dtypes = {
            'STATION' : 'string',
            'DATE' : 'datetime64[ns]',
            'SOURCE' : 'string',
            'LATITUDE' : 'float64',
            'LONGITUDE' : 'float64',
            'ELEVATION' : 'float64',
            'NAME' : 'string',
            'REPORT_TYPE' : 'string',
            'CALL_SIGN' : 'string',
            'QUALITY_CONTROL' : 'string'
        } | {k : 'string' for k in [
            'WND','CIG','VIS','TMP','DEW','SLP','AA1','AA2','AA3','AB1','AD1',
            'AE1','AH1','AH2','AH3','AH4','AH5','AH6','AI1','AI2','AI3','AI4',
            'AI5','AI6','AJ1','AL1','AN1','AT1','AT2','AT3','AT4','AU1','AU2',
            'AU3','AW1','AW2','AW3','AW4','AX1','AX2','AX3','GA1','GA2','GA3',
            'GD1','GD2','GD3','GE1','GF1','KA1','KA2','KB1','KB2','KB3','KC1',
            'KC2','KD1','KD2','KE1','KG1','KG2','MA1','MD1','MF1','MG1','MH1',
            'MK1','MV1','MW1','OC1','OD1','OE1','OE2','OE3','RH1','RH2','RH3',
            'REM','EQD'
        ]}
        normalized_metrics = [
            'HLY-TEMP-NORMAL',
            'HLY-TEMP-10PCTL',
            'HLY-TEMP-90PCTL',
            'HLY-DEWP-NORMAL',
            'HLY-DEWP-10PCTL',
            'HLY-DEWP-90PCTL',
            'HLY-PRES-NORMAL',
            'HLY-PRES-10PCTL',
            'HLY-PRES-90PCTL',
            'HLY-CLDH-NORMAL',
            'HLY-HTDH-NORMAL',
            'HLY-CLOD-PCTCLR',
            'HLY-CLOD-PCTFEW',
            'HLY-CLOD-PCTSCT',
            'HLY-CLOD-PCTBKN',
            'HLY-CLOD-PCTOVC',
            'HLY-HIDX-NORMAL',
            'HLY-WCHL-NORMAL',
            'HLY-WIND-AVGSPD',
            'HLY-WIND-PCTCLM',
            'HLY-WIND-VCTDIR',
            'HLY-WIND-VCTSPD',
            'HLY-WIND-1STDIR',
            'HLY-WIND-1STPCT',
            'HLY-WIND-2NDDIR',
            'HLY-WIND-2NDPCT'
        ]
        self.normalized_weather_data_dtypes = {
            'STATION' : 'string',
            'NAME' : 'string',
            'LATITUDE' : 'float64',
            'LONGITUDE' : 'float64',
            'ELEVATION' : 'float64',
            'DATE' : 'datetime64[ns]',
            'month' : 'int64',
            'day' : 'int64',
            'hour' : 'int64'
        } | {prefix+metric:dtype for metric in normalized_metrics for (prefix,dtype) in (('','float64'),('meas_flag_','string'),('comp_flag_','string'),('years_','int64'))}

        log_path = Path(config['weather_data']['download_log_path'])

        self.weather_station_information_url = config['weather_data']['urls']['weather_station_information']
        self.normalized_weather_inventory_url = config['weather_data']['urls']['normalized_weather_inventory']
        self.historic_data_url_template = config['weather_data']['urls']['historic_data_template']
        self.normalized_weather_year_url_template = config['weather_data']['urls']['normalized_weather_year_template']
        self.historic_data_download_path_template = config['weather_data']['download_path_templates']['historic_data']
        self.normalized_weather_year_download_path_template = config['weather_data']['download_path_templates']['normalized_weather_year']
        self.combined_reports_path_template = config['weather_data']['combined_reports_path_template']
        self.years = config['ucap_analysis']['years']
        self.logger = DataLogger(dtypes=log_dtypes,log_path=log_path,delimiter=',')
        self.status_logger = TextLogger(
            cli_logging_criticalities=['INFORMATION','WARNING','ERROR'],
            file_logging_criticalities=['WARNING','ERROR'],
            log_path=config['weather_data']['text_log_path']
        )

    def get_historic_data_url(self,usaf:str,wban:str,year:int):
        '''
        Generates the url to a NCEI/NOAA hourly global surface temperature data
        file based on a given weather station id and year.

        Parameters:
            usaf - a 6-digit US Air Force station ID as specified in the
                isd-history.txt file on the NCEI/NOAA website.
            wban - a 5-digit NCDC WBAN number as specified in the
                isd-history.txt file on the NCEI/NOAA website.
            year - a current or past year for which weather data is requested,
                expressed as an integer.
        '''
        url = replace_template_placeholders(
            self.historic_data_url_template,
            {
                'year' : str(year),
                'file_id' : f'{usaf}{wban}'
            }
        )
        return url
    
    def get_normalized_weather_year_url(self,usaf:str,wban:str):
        '''
        Generates the url to a NCEI/NOAA hourly normalized weather year based on
        a given weather station id and year.

        Parameters:
            usaf - a 6-digit US Air Force station ID as specified in the
                isd-history.txt file on the NCEI/NOAA website.
            wban - a 5-digit NCDC WBAN number as specified in the
                isd-history.txt file on the NCEI/NOAA website.
            year - a current or past year for which weather data is requested,
                expressed as an integer.
        '''
        url = replace_template_placeholders(
            self.normalized_weather_year_url_template,
            {
                'country' : 'US',
                'wban' : ('0000000'+wban)[-8:]
            }
        )
        return url

    def get_weather_station_information(self):
        '''
        Retrieves a text file from the NOAA website containing information about
        each weather station available in the hourly surface temperature data
        files and parses the data contained in the file into a dataframe.
        '''
        if self.weather_station_information.empty:
            url = self.weather_station_information_url
            buffer = io.BytesIO()
            columns = {
                'USAF' : 'string',
                'WBAN' : 'string',
                'STATION NAME' : 'string',
                'CTRY' : 'string',
                'ST' : 'string',
                'CALL' : 'string',
                'LAT' : 'float64',
                'LON' : 'float64',
                'ELEV(M)' : 'float64',
                'BEGIN' : 'datetime64[ns]',
                'END' : 'datetime64[ns]'
            }
            try:
                c = pycurl.Curl()
                c.setopt(c.URL,url)
                c.setopt(c.WRITEDATA,buffer)
                c.perform()
                c.close()

                buffer.seek(0)
                re_str = '\s+'.join([s.replace('(',r'\(').replace(')',r'\)') for s in columns.keys()])
                header_row_found = False
                position = 0
                while not header_row_found:
                    position = buffer.tell()
                    line = buffer.readline().decode('utf-8')
                    header_row_found = re.match(re_str,line)
                buffer.seek(position)
                df = pd.read_fwf(buffer)
                buffer.close()

                # Convert BEGIN and END columns to datetime (format: YYYYMMDD)
                df['BEGIN'] = pd.to_datetime(df['BEGIN'].astype('string'))
                df['END'] = pd.to_datetime(df['END'].astype('string'))

                # Explicate column types:
                for column_name,column_dtype in columns.items():
                    df[column_name] = df[column_name].astype(column_dtype)
                
                # Add zero-padding to WBAN column:
                df['WBAN'] = df['WBAN'].map(lambda s: ('00000'+s)[-5:])

                # load weather station information to object for sharing across
                # class methods:
                self.weather_station_information = df.copy()

            except:
                df = pd.DataFrame(columns=columns.keys())
        else:
            df = self.weather_station_information.copy()

        return df

    def get_normalized_weather_inventory(self):
        '''
        Retrieves a text file from the NOAA website containing information about
        each weather station available in the 1991-2020 normalized weather year
        dataset and parses the data contained in the file into a dataframe.
        '''
        if self.normalized_weather_inventory.empty:
            url = self.normalized_weather_inventory_url
            buffer = io.BytesIO()
            try:
                c = pycurl.Curl()
                c.setopt(c.URL,url)
                c.setopt(c.WRITEDATA,buffer)
                c.perform()
                c.close()

                buffer.seek(0)
                columns = {
                    'ID' : 'string',
                    'LAT' : 'float64',
                    'LON' : 'float64',
                    'ELEV(M)': 'float64',
                    'ST' : 'string',
                    'STATION NAME' : 'string',
                    'GSN' : 'string',
                    'HCN' : 'string',
                    'USAF' : 'float64'
                }
                df = pd.read_fwf(buffer,names=columns.keys())
                buffer.close()

                # Explicate column types:
                for column_name,column_dtype in columns.items():
                    df.loc[:,column_name] = df.loc[:,column_name].astype(column_dtype)

                # load weather station information to object for sharing across
                # class methods:
                self.normalized_weather_inventory = df.copy()

            except:
                df = pd.DataFrame(columns=columns.keys())
        else:
            df = self.normalized_weather_inventory.copy()
        return df

    def select_nearest_weather_station(self,lat:float,lon:float):
        '''
        Returns the nearest weather station available in the NOAA data set to
        the input latitude and longitude for which normalized weather years are
        available.

        Parameters:
            lat - latitude in decimal degrees, with [0,90] being North and
                [-90,0) being South
            lon - longitude in decimal degrees, with [0,180] being East and
                (-180,0) being West
        '''

        # Retrieve normalized weather inventory from the NCEI/NOAA website:
        normalized_weather_inventory = self.get_normalized_weather_inventory()

        # Retrieve weather station information from the NCEI/NOAA website:
        weather_station_information = self.get_weather_station_information()

        # Calculate distance between input coordinates and each station:
        normalized_weather_inventory['DISTANCE'] = normalized_weather_inventory.apply(
            lambda r: geodesic_distance((r['LAT'],r['LON']),(lat,lon)),
            axis='columns',
            result_type='expand'
        ).round(3)

        normalized_weather_inventory = normalized_weather_inventory.sort_values(by='DISTANCE',ascending=True)

        # Find and return the closest weather station to input coordinates:
        min_distance = normalized_weather_inventory.iloc[0]['DISTANCE']
        id = normalized_weather_inventory.iloc[0]['ID']
        wban = id[-5:]
        usaf = normalized_weather_inventory.iloc[0]['USAF']
        ws_lat = normalized_weather_inventory.iloc[0]['LAT']
        ws_lon = normalized_weather_inventory.iloc[0]['LON']

        if wban in list(weather_station_information['WBAN']):
            weather_station = weather_station_information.loc[
                weather_station_information['WBAN']==wban,
                :
            ].sort_values(by='END',ascending=False).iloc[0]
        else:
            self.status_logger.log('No weather station found for resource {resource_id}','WARNING')
            weather_station = pd.Series({k:'' for k in weather_station_information.columns})

        weather_station['DISTANCE'] = min_distance
        return weather_station

    def get_historic_data_download_path(self,usaf:str,wban:str,year:int):
        '''
        Defines a default path for data files based on the weather station data
        and year of its contents.

        Parameters:
            usaf - a 6-digit US Air Force station ID as specified in the
                isd-history.txt file on the NCEI/NOAA website.
            wban - a 5-digit NCDC WBAN number as specified in the
                isd-history.txt file on the NCEI/NOAA website.
            year - a current or past year expressed as an integer.
        '''
        return Path(
            replace_template_placeholders(
                self.historic_data_download_path_template,
                {
                    'usaf' : usaf,
                    'wban' : wban,
                    'year' : str(year)
                }
            )
        )

    def get_normalized_weather_year_download_path(self,usaf:str,wban:str):
        '''
        Defines a default path for normalized weather year files based on the
        weather station of its contents.

        Parameters:
            usaf - a 6-digit US Air Force station ID as specified in the
                isd-history.txt file on the NCEI/NOAA website.
            wban - a 5-digit NCDC WBAN number as specified in the
                isd-history.txt file on the NCEI/NOAA website.
            year - a current or past year expressed as an integer.
        '''
        return Path(
            replace_template_placeholders(
                self.normalized_weather_year_download_path_template,
                {
                    'usaf' : usaf,
                    'wban' : wban,
                }
            )
        )

    def download_historic_weather_data_by_callsign(
        self,
        weather_station_callsign:str,
        year:int,
        overwrite:bool=False
    ):
        '''
        Downloads and saves a weather data file from the NCEI/NOAA hourly global
        surface temperature database based on the input weather station and
        year. When requesting weather data for the current year, only a partial
        year of data is retrieved.

        Parameters:
            weather_station_callsign - a unique four-letter abbreviation for a
                weather station corresponding to a row in the
                weather_station_placenames.csv file used for identifying data
                files on the NCEI/NOAA repository.
            year - a Pandas timestamp with a current or past year for which
                weather data is requested.
            overwrite - a boolean value indicating whether files already
                downloaded should be overwritten. Default value is True.
        '''
        weather_station_information = self.get_weather_station_information()
        weather_station = weather_station_information.loc[
            (weather_station_information.loc[:,'CALL']==weather_station_callsign),
            :
        ].sort_values('BEGIN').iloc[-1]

        usaf = weather_station.loc['USAF']
        wban = weather_station.loc['WBAN']
        self.download_historic_weather_data(usaf,wban,year,overwrite)

    def download_historic_weather_data(
        self,
        usaf:str,
        wban:str,
        year:int,
        overwrite:bool=False
    ):
        '''
        Downloads and saves an annual weather data file from the NCEI/NOAA
        hourly global surface temperature database based on the input file_id,
        consisting of the 6-digit USAF and 5-digit WBAN numbers. When requesting
        weather data for the current year, only a partial year of data is retrieved.

        Parameters:
            usaf - a weather station's 6-digit US Air Force station ID
                as specified in the isd-history.txt file on the NCEI/NOAA
                website
            wban - a weather station's 5-digit NCDC WBAN number as specified in
                the isd-history.txt file on the NCEI/NOAA website
            year - a Pandas timestamp with a current or past year for which
                weather data is requested.
            overwrite - a boolean value indicating whether files already
                downloaded should be overwritten. Default value is True.
        '''
        usaf = ('000000' + usaf)[-6:]
        wban = ('00000' + wban)[-5:]
        weather_station_information = self.get_weather_station_information()
        callsign = weather_station_information.loc[
            (weather_station_information['USAF']==usaf) & \
            (weather_station_information['WBAN']==wban),
            'CALL'
        ].iloc[0]
        url = self.get_historic_data_url(usaf,wban,year)
        download_path = self.get_historic_data_download_path(usaf,wban,year)
        if (
            (self.logger.data['effective_date']==year) & \
            (self.logger.data['type']=='historic') & \
            (self.logger.data['usaf']==usaf) & \
            (self.logger.data['wban']==wban)
        ).any() and not overwrite:
            filename = download_path.name
            self.status_logger.log(f'Skipping file already downloaded: {filename}','INFORMATION')
        else:
            try:
                download_path.parent.mkdir(parents=True,exist_ok=True)
                with download_path.open('wb') as f:
                    c = pycurl.Curl()
                    c.setopt(c.URL,url)
                    c.setopt(c.WRITEDATA,f)
                    c.perform()
                    c.close()
                    self.logger.log(pd.Series({
                        'effective_date' : dt(year,1,1),
                        'usaf': usaf,
                        'wban' : wban,
                        'callsign' : callsign,
                        'type' : 'historic',
                        'source_url' : url,
                        'download_path' : str(download_path),
                        'loaded_to_parquet' : 0,
                    }))
                    self.logger.commit()
                    self.status_logger.log(f'Downloaded {download_path}','INFORMATION')
            except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
                self.status_logger.log('Specified File Not Available at Given URL','ERROR')

    def download_normalized_weather_data(self,usaf:str,wban:str,overwrite:bool=False):
        '''
        Downloads and saves a normalized weather year data file from the NCEI/
        NOAA based on the input file_id, consisting of the 6-digit USAF and 5-
        digit WBAN numbers. When requesting weather data for the current year, only a partial year of data is retrieved.

        Parameters:
            usaf - a weather station's 6-digit US Air Force station ID
                as specified in the isd-history.txt file on the NCEI/NOAA
                website
            wban - a weather station's 5-digit NCDC WBAN number as specified in
                the isd-history.txt file on the NCEI/NOAA website
            overwrite - a boolean value indicating whether files already
                downloaded should be overwritten. Default value is True.
        '''
        usaf = ('000000' + usaf)[-6:]
        wban = ('00000' + wban)[-5:]
        weather_station_information = self.get_weather_station_information()
        callsign = weather_station_information.loc[
            (weather_station_information['USAF']==usaf) & \
            (weather_station_information['WBAN']==wban),
            'CALL'
        ].iloc[0]
        url = self.get_normalized_weather_year_url(usaf,wban)
        download_path = self.get_normalized_weather_year_download_path(usaf,wban)
        if (
            (self.logger.data['type']=='normalized') & \
            (self.logger.data['usaf']==usaf) & \
            (self.logger.data['wban']==wban)
        ).any() and not overwrite:
            filename = download_path.name
            self.status_logger.log(f'Skipping file already downloaded: {filename}','INFORMATION')
        else:
            try:
                download_path.parent.mkdir(parents=True,exist_ok=True)
                with download_path.open('wb') as f:
                    c = pycurl.Curl()
                    c.setopt(c.URL,url)
                    c.setopt(c.WRITEDATA,f)
                    c.perform()
                    c.close()
                    self.logger.log(pd.Series({
                        'effective_date' : dt(1900,1,1),
                        'usaf': usaf,
                        'wban' : wban,
                        'callsign' : callsign,
                        'type' : 'normalized',
                        'source_url' : url,
                        'download_path' : str(download_path),
                        'loaded_to_parquet' : 0,
                    }))
                    self.logger.commit()
                    self.status_logger.log(f'Downloaded {download_path}','INFORMATION')
            except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError):
                self.status_logger.log('Specified File Not Available at Given URL','ERROR')

    def get_historic_weather_data(self,usaf:str,wban:str,year:int):
        '''
        Returns a dataframe with data from the input weather station for the
        requested year, first downloading from the NCEI/NOAA website if not
        already available.

        Parameters:
            usaf - a weather station's 6-digit US Air Force station ID
                as specified in the isd-history.txt file on the NCEI/NOAA
                website
            wban - a weather station's 5-digit NCDC WBAN number as specified in
                the isd-history.txt file on the NCEI/NOAA website
            year - a Pandas timestamp with a current or past year for which
                weather data is requested.
        '''
        # Create station id from usaf and wban:
        station_id = ('00000' + usaf)[-6:] + ('0000' + wban)[-5:]

        # Check whether file should exist:
        weather_station_information = self.get_weather_station_information()
        weather_station = weather_station_information.loc[
            (weather_station_information['USAF']==usaf) &
            (weather_station_information['WBAN']) &
            (weather_station_information['BEGIN']<=dt(year,1,1)) &
            (weather_station_information['END']>=dt(year,1,1)),
            :
        ]
        if len(weather_station.index)>0:
            # Check whether file has been downloaded
            if len(self.logger.data.loc[
                (self.logger.data['usaf']==usaf) &
                (self.logger.data['wban']==wban) &
                (self.logger.data['type']=='historic') &
                (self.logger.data['effective_date']==dt(year,1,1)),
                :
            ].index)>0:
                # Check whether downloaded file has been loaded into parquet:
                if self.logger.data.loc[
                    (self.logger.data['usaf']==usaf) &
                    (self.logger.data['wban']==wban) &
                    (self.logger.data['type']=='historic') &
                    (self.logger.data['effective_date']==dt(year,1,1)),
                    'loaded_to_parquet'
                ].iloc[0]==0:
                    self.update_parquet()
            else:
                # If file has not been downloaded, download then load to parquet:
                self.download_historic_weather_data(usaf,wban,year)
                self.update_parquets()

            # Load data from parquet and filter for requested data:
            df = self.load_parquet('historic')
            df = df.loc[
                (df['STATION']==station_id) &
                (df['DATE'].dt.year==year)
            ]
        else:
            # Weather station not found or unavailable or requested year:
            self.status_logger.log('Weather data unavailable',criticality='INFORMATION')
            df = pd.DataFrame()

        return df

    def get_normalized_weather_data(self,usaf:str,wban:str):
        '''
        Returns a dataframe with normalized weather data from the input weather
        station, first downloading from the NCEI/NOAA website if not already
        available.

        Parameters:
            usaf - a weather station's 6-digit US Air Force station ID
                as specified in the isd-history.txt file on the NCEI/NOAA
                website
            wban - a weather station's 5-digit NCDC WBAN number as specified in
                the isd-history.txt file on the NCEI/NOAA website
        '''
        # Check whether file should exist:
        weather_station_information = self.get_normalized_weather_inventory()

        # Generate station id based on input WBAN:
        station_id = 'USW' + ('000000' + wban)[-8:]

        weather_station = weather_station_information.loc[
            (weather_station_information.loc[:,'ID']==station_id),
            :
        ]
        if len(weather_station.index)>0:
            # Check whether file has been downloaded
            if len(self.logger.data.loc[
                (self.logger.data['wban']==wban) &
                (self.logger.data['type']=='normalized'),
                :
            ].index)>0:
                # Check whether downloaded file has been loaded into parquet:
                if not self.logger.data.loc[
                    (self.logger.data['wban']==wban) &
                    (self.logger.data['type']=='normalized'),
                    'loaded_to_parquet'
                ].iloc[0]:
                    self.update_parquet()
            else:
                # If file has not been downloaded, download then load to parquet:
                self.download_normalized_weather_data(usaf,wban)
                self.update_parquets()

            # Load data from parquet and filter for requested data:
            df = self.load_parquet('normalized')
            df = df.loc[
                (df['STATION']==station_id),
                :
            ]
        else:
            # Weather station not found or unavailable or requested year:
            self.status_logger.log('Weather data unavailable',criticality='INFORMATION')
            df = pd.DataFrame()

        return df

    def download_all_files(self,overwrite:bool=False):
        '''
        Downloads all data files for the weather stations and years specified
        in the object attributes, and saves to the default locations.
        '''
        errors = []
        weather_station_information = self.get_weather_station_information()
        for year in self.years:
            for weather_station in weather_station_information.loc[:,'CALL']:
                output_path = self.get_historic_data_download_path(weather_station,year)
                try:
                    self.download_historic_weather_data(weather_station,year,output_path,overwrite)
                except (requests.exceptions.HTTPError,requests.exceptions.ConnectionError):
                    errors += [f'{weather_station} - {year}']
        if len(errors)>0:
            self.status_logger.log('Unable to retrieve data files for the following weather stations and years:\n\t' + '\n\t'.join(errors),'ERROR')
        self.status_logger.log('Downloads complete!','INFORMATION')
    
    def delete_weather_data(self,usaf:int,wban:int,year:int):
        '''
        Deletes a downloaded weather file corresponding to the input
        weather_station_id and yearand removes its record from the log.

        Parameters:
            usaf - a weather station's 6-digit US Air Force station ID
                as specified in the isd-history.txt file on the NCEI/NOAA
                website
            wban - a weather station's 5-digit NCDC WBAN number as specified in
                the isd-history.txt file on the NCEI/NOAA website
            year - a current or previous year for which data should be read
        '''
        download_record = self.logger.data.loc[
            (self.logger.data.loc[:,'usaf']==usaf)&
            (self.logger.data.loc[:,'wban']==wban)&
            (self.logger.data.loc[:,'effective_date']==dt(year,1,1)),
            :
        ].iloc[0]
        download_path = Path(download_record.loc['download_path'])
        self.status_logger.log(f'Removing Weather Data: {usaf}-{wban} {year}')
        download_path.unlink()
        df = self.load_parquet(type)
        df.drop(
            index=df.loc[
                (df.loc[:,'STATION']==usaf+wban)&
                (df.loc[:,'DATE'].map(lambda d:d.year)==year),
                :
            ].index,
            inplace=True
        )
        self.dump_parquets(df)
        self.logger.data.drop(
            index=self.logger.data.loc[
                (self.logger.data.loc[:,'usaf']==usaf) &
                (self.logger.data.loc[:,'wban']==wban) &
                (self.logger.data.loc[:,'effective_date']==dt(year,1,1)),
                :
            ].index,
            inplace=True
        )
        self.logger.commit()

    def read_historic_weather_file(self,usaf:str,wban:str,year:int):
        '''
        Reads the weather data file for the input weather station and year,
        first downloading the file if unavailable.

        Parameters:
            usaf - a weather station's 6-digit US Air Force station ID
                as specified in the isd-history.txt file on the NCEI/NOAA
                website
            wban - a weather station's 5-digit NCDC WBAN number as specified in
                the isd-history.txt file on the NCEI/NOAA website
            year - a current or previous year for which data should be read
        
        Returns:
            DataFrame containing the weather data contained in the file
            corresponding to the input weather station and year.
        '''
        download_path = self.get_historic_data_download_path(usaf,wban,year)
        if not download_path.is_file():
            self.download_historic_weather_data(usaf,wban,year,download_path)
        df = pd.read_csv(download_path)
        for k,v in self.historic_weather_data_dtypes.items():
            if k in df.columns:
                df.loc[:,k] = df.loc[:,k].astype(v)
        df['DATE'] = df['DATE'].dt.tz_localize('UTC').dt.tz_convert('Etc/GMT+8')
        df['TEMPERATURE DEGC'] = df['TMP'].map(lambda s:float(s.split(',')[0])/10)
        df['TEMPERATURE QC'] = df['TMP'].map(lambda s:s.split(',')[1])
        df['TYPE'] = 'historic'
        return df[['STATION','DATE','LATITUDE','LONGITUDE','ELEVATION','TYPE','TEMPERATURE DEGC','TEMPERATURE QC']]

    def read_normalized_weather_file(self,usaf:str,wban:str):
        '''
        Reads the normalized weather data file for the input weather station,
        first downloading the file if unavailable.

        Parameters:
            usaf - a weather station's 6-digit US Air Force station ID
                as specified in the isd-history.txt file on the NCEI/NOAA
                website
            wban - a weather station's 5-digit NCDC WBAN number as specified in
                the isd-history.txt file on the NCEI/NOAA website
        
        Returns:
            DataFrame containing the normalized weather year contained in the
            file corresponding to the input weather station.
        '''
        download_path = self.get_normalized_weather_year_download_path(usaf,wban)
        if not download_path.is_file():
            self.download_normalized_weather_data(usaf,wban,download_path)
        df = pd.read_csv(download_path)
        df['DATE'] = df['DATE'].map(lambda s: dt(1900,int(s[0:2]),int(s[3:5]),int(s[6:8])))
        df['DATE'].dt.tz_localize('Etc/GMT+8')
        for k,v in self.normalized_weather_data_dtypes.items():
            if k in df.columns:
                df.loc[:,k] = df.loc[:,k].astype(v)
        df['TEMPERATURE DEGC'] = (df['HLY-TEMP-NORMAL'] - 32 ) * 5 / 9
        df['TEMPERATURE QC'] = df['comp_flag_HLY-TEMP-NORMAL']
        df['TYPE'] = 'normalized'
        return df[['STATION','DATE','LATITUDE','LONGITUDE','ELEVATION','TYPE','TEMPERATURE DEGC','TEMPERATURE QC']]

    def load_parquet(self,type:str):
        '''
        Loads weather data from a saved parquet file to reduce time reading from
        .csv files

        Parameters:
            type - either 'normalized' or 'historic'
        '''
        combined_reports_path = Path(replace_template_placeholders(
            self.combined_reports_path_template,
            {'type' : type}
        ))
        if combined_reports_path.is_file():
            return pd.read_parquet(combined_reports_path)
        else:
            return pd.DataFrame(columns=['STATION','DATE','LATITUDE','LONGITUDE','ELEVATION','TYPE','TEMPERATURE DEGC','TEMPERATURE QC'])

    def dump_parquets(self,weather_data):
        '''
        Saves the input dataframe of weather data to a parquet file at
        the path specified in the config dictionary

        Parameters:
            weather_data - a Pandas dataframe containing either normalized or
                historic weather data to be appended to the parquet file
            type - either 'normalized' or 'historic'
        '''
        for type in list(weather_data['TYPE'].unique()):
            combined_reports_path = replace_template_placeholders(
                self.combined_reports_path_template,
                {'type' : type}
            )
            weather_data.to_parquet(combined_reports_path)

    def clear_parquets(self):
        '''
        Deletes the parquet files containing combined curtailment reports and
        sets the value of the 'loaded_to_parquet' column in the log to 0 for all
        downloaded reports
        '''
        years = self.logger.data.loc[:,'effective_date'].map(lambda x:x.year).unique()
        weather_stations = self.logger.data.loc[:,['usaf','wban']].drop_duplicates()
        for type in ('normalized','historic'):
            combined_reports_path = Path(replace_template_placeholders(
                self.combined_reports_path_template,
                {'type' : type}
            ))
            combined_reports_path.unlink()
            self.logger.data.loc[
                (self.logger.data['type']==type),
                'loaded_to_parquet'
            ] = 0
        self.logger.commit()

    def update_parquets(self):
        '''
        Loads data from the combined weather data parquets file, then extracts
        any downloaded reports not already loaded and saves the updated
        DataFrame to the parquet file.
        '''
        unloaded_reports = self.logger.data.loc[(self.logger.data.loc[:,'loaded_to_parquet']==0),:]
        weather_stations = unloaded_reports[['usaf','wban']].drop_duplicates()
        for _,unloaded_report in unloaded_reports.iterrows():
            usaf = unloaded_report['usaf']
            wban = unloaded_report['wban']
            type = unloaded_report['type']
            effective_date = unloaded_report['effective_date']
            df = self.load_parquet(type)
            if type=='historic':
                new_data = self.read_historic_weather_file(usaf,wban,effective_date.year)
            elif type=='normalized':
                new_data = self.read_normalized_weather_file(usaf,wban)
            df = pd.concat([df,new_data],ignore_index=True)
            self.logger.data.loc[
                (self.logger.data['usaf']==usaf) &
                (self.logger.data['wban']==wban) &
                (self.logger.data['type']==type) &
                (self.logger.data['effective_date']==effective_date),
                'loaded_to_parquet'
            ]=1
            self.dump_parquets(df)
        self.logger.commit()