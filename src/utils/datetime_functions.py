from datetime import datetime, timedelta
from pandas import DataFrame

def datetime_range_overlap(tr0_0:datetime,tr0_1:datetime,tr1_0:datetime,tr1_1:datetime):
    '''
    Calculate the duration of overlap between two datetime ranges, defined by
    the start and end dates and times, in hours.

        parameters:
            tr0_0 - a datetime object specifying the start date and time for the
                first range
            tr0_1 - a datetime object specifying the end date and time for the
                first range
            tr1_0 - a datetime object specifying the start date and time for the
                second range
            tr1_1 - a datetime object specifying the end date and time for the
                second range
        returns:
            a floating point value representing the duration in hours of overlap
            between the two input datetime ranges.
    '''
    overlap = max( min(tr0_1, tr1_1) - max(tr0_0, tr1_0), timedelta(hours=0))
    return overlap.total_seconds() / 3600

def hour_filter_overlap(tr_0:datetime,tr_1:datetime,hour_filter:DataFrame):
    '''
    Calculates the total overlapping duration between a datetime range and a
    dataframe of blocks of hours.

        parameters:
            tr_0 - a datetime object specifying the start date and time for the
                first range
            tr_1 - a datetime object specifying the end date and time for the
                first range
            hour_filter - a dataframe of datetime objects representing blocks of
                hours specified in 'START DATETIME' and 'END DATETIME' columns to compare the datetime range against
        
        returns:
            a floating point value representing the duration in hours of overlap
            between the input datetime range and list of hours.
    '''
    overlap = 0
    applicable_hour_filter = hour_filter.loc[
        (hour_filter.loc[:,'START DATETIME']<=tr_1) & \
        (hour_filter.loc[:,'END DATETIME']>=tr_0),
        :
    ]
    for _,r in applicable_hour_filter.iterrows():
        overlap += datetime_range_overlap(
            tr_0,
            tr_1,
            r.loc['START DATETIME'].to_pydatetime(),
            r.loc['END DATETIME'].to_pydatetime()
        )
    return overlap

def select_hours_within_datetime_range(tr_0:datetime,tr_1:datetime,hour_filter:DataFrame):
    '''
    Extracts a subset of hours from a list of hours which fall within a datetime
    range.

        parameters:
            tr_0 - a datetime object specifying the start date and time for the
                first range
            tr_1 - a datetime object specifying the end date and time for the
                first range
            hour_filter - a pandas dataframe with columns labeled
                'START DATETIME', 'END DATETIME' and 'DEMAND HOUR', and
                optionally 'RESOURCE ID', indicating selected hours, generally
                expected to constitute a year of hours.
        
        returns:
            a list of hours selected from the input list containing only hours
            within the datetime range bounded by the input datetimes.
    '''
    return hour_filter.loc[
        (hour_filter.loc[:,'START DATETIME']>=tr_0) & \
        (hour_filter.loc[:,'END DATETIME']<=tr_1),
        :
    ].reset_index()

def coalesce_hour_filter(hour_filter:DataFrame):
    '''
    Combines contiguous hours with the same Demand Hour value (true or false)
    within the input hour filter.

        parameters:
            hour_filter - a pandas dataframe with columns labeled
                'START DATETIME', 'END DATETIME', 'DEMAND HOUR', and optionally
                'RESOURCE ID', indicating selected hours, generally expected to
                constitute a year of hours.
        
        returns:
            a pandas dataframe with columns labeled 'START DATETIME',
            'END DATETIME', 'DEMAND HOUR', and 'RESOURCE ID' if present in the input
            representing the start and end datetimes for each contiguous block
            of filter hours
    '''

    # Ensure data are sequential:
    columns = list(filter(lambda s: s in hour_filter.columns,['RESOURCE ID','START DATETIME']))
    hour_filter.sort_values(by=columns,axis='index',ascending=True,inplace=True)

    # Reset index:
    hour_filter.reset_index(inplace=True)
    columns = filter(lambda s:s in hour_filter.columns,['RESOURCE ID','START DATETIME','END DATETIME','SEASON','DEMAND HOUR'])
    hour_filter = hour_filter.loc[:,columns]

    # Iterate through hours to find consecutive blocks with the same DEMAND HOUR
    # value:
    use_resource_id = 'RESOURCE ID' in hour_filter.columns
    if use_resource_id:
        for i in hour_filter.index:
            if i<max(hour_filter.index) and \
                hour_filter.loc[i,'RESOURCE ID']==hour_filter.loc[i+1,'RESOURCE ID'] and \
                hour_filter.loc[i,'END DATETIME']==hour_filter.loc[i+1,'START DATETIME'] and \
                hour_filter.loc[i,'DEMAND HOUR']==hour_filter.loc[i+1,'DEMAND HOUR']:
                    hour_filter.loc[i+1,'START DATETIME'] = hour_filter.loc[i,'START DATETIME']
            else:
                pass
    else:
        for i in hour_filter.index:
            if i<max(hour_filter.index) and \
                hour_filter.loc[i,'END DATETIME']==hour_filter.loc[i+1,'START DATETIME'] and \
                hour_filter.loc[i,'DEMAND HOUR']==hour_filter.loc[i+1,'DEMAND HOUR']:
                    hour_filter.loc[i+1,'START DATETIME'] = hour_filter.loc[i,'START DATETIME']
            else:
                pass

    # Aggregate hours into blocks with start and end datetimes:
    if use_resource_id:
        hour_filter = hour_filter.loc[
            :,
            ['RESOURCE ID','START DATETIME','END DATETIME','DEMAND HOUR']
        ].groupby(
            ['RESOURCE ID','START DATETIME','DEMAND HOUR']
        ).max().reset_index().loc[
            :,
            ['RESOURCE ID','START DATETIME','END DATETIME','DEMAND HOUR']
        ]
    else:
        hour_filter = hour_filter.loc[
            :,
            ['START DATETIME','END DATETIME','DEMAND HOUR']
        ].groupby(
            ['START DATETIME','DEMAND HOUR']
        ).max().reset_index().loc[
            :,
            ['START DATETIME','END DATETIME','DEMAND HOUR']
        ]

    return hour_filter