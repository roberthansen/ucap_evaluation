from datetime import datetime as dt

def get_economic_bid(start_datetime:dt=None,end_datetime:dt=None,resource_id:str=None):
    columns = [
        'ResID',
        'UNIT_TYPE',
        'DateTime',
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
    columns_str = ','.join([f'"{c}"' for c in columns])
    if resource_id is not None:
        if start_datetime is not None and end_datetime is not None:
            start_datetime_str = start_datetime.isoformat()
            end_datetime_str = end_datetime.isoformat()
            sql_str = f'''PREPARE economic_bid AS
            SELECT {columns_str}
            FROM caisobiddingdata
            WHERE "ResID"=$1 AND "DateTime">=$2 AND "DateTime"<$3;
            EXECUTE economic_bid('{resource_id}','{start_datetime}','{end_datetime}');'''
        elif start_datetime is not None:
            start_datetime_str = start_datetime.isoformat()
            sql_str = f'''PREPARE economic_bid AS
            SELECT {columns_str}
            FROM caisobiddingdata
            WHERE "ResID"=$1 AND "DateTime">=$2;
            EXECUTE economic_bid('{resource_id}','{start_datetime}');'''
        elif end_datetime is not None:
            end_datetime_str = end_datetime.isoformat()
            sql_str = f'''PREPARE economic_bid AS
            SELECT {columns_str}
            FROM caisobiddingdata
            WHERE "ResID"=$1 AND "DateTime"<$2;
            EXECUTE economic_bid('{resource_id}','{end_datetime}');'''
        else:
            sql_str = f'''PREPARE economic_bid AS
            SELECT {columns_str}
            FROM caisobiddingdata
            WHERE "ResID"=$1;
            EXECUTE economic_bid('{resource_id}');'''
    else:
        if start_datetime is not None and end_datetime is not None:
            start_datetime_str = start_datetime.isoformat()
            end_datetime_str = end_datetime.isoformat()
            sql_str = f'''PREPARE economic_bid AS
            SELECT {columns_str}
            FROM caisobiddingdata
            WHERE "DateTime">=$1 AND "DateTime"<$2;
            EXECUTE economic_bid('{start_datetime}','{end_datetime}');'''
        elif start_datetime is not None:
            start_datetime_str = start_datetime.isoformat()
            sql_str = f'''PREPARE economic_bid AS
            SELECT {columns_str}
            FROM caisobiddingdata
            WHERE "DateTime">=$1;
            EXECUTE economic_bid('{start_datetime}');'''
        elif end_datetime is not None:
            end_datetime_str = end_datetime.isoformat()
            sql_str = f'''PREPARE economic_bid AS
            SELECT {columns_str}
            FROM caisobiddingdata
            WHERE "DateTime"<$1;
            EXECUTE economic_bid('{end_datetime}');'''
        else:
            sql_str = f'''PREPARE economic_bid AS
            SELECT {columns_str}
            FROM caisobiddingdata
            WHERE "DateTime">=$1 AND "DateTime"<$2;
            EXECUTE economic_bid();'''
    return sql_str

def get_master_capability_list():
    sql_str = '''
        SELECT
            *
        FROM caisomastercapability_current
    '''
    return sql_str

def get_master_file():
    sql_str = '''
        SELECT
            *
        FROM caisomasterfile_current
    '''
    return sql_str