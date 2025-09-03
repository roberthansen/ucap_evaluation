import psycopg2
import numpy as np
import pandas as pd
from pathlib import Path
from pandas import Timestamp as ts,Timedelta as td
from pandas.tseries.offsets import MonthBegin,MonthEnd
from login import pguser


resource_ids=pd.read_csv(r'M:\Users\RH2\src\caiso_curtailments\ResourceTypes_MRD2024-06.csv')

s='''
WITH resource_types ("ResourceID","ResourceType") AS ( VALUES
    {}
)

SELECT
"RESOURCE_TYPE",
"DateTime",
SUM("MWh") AS "MWH"
FROM caisosettlementdata as a
LEFT JOIN resource_types as b
on a."ResID"=b."ResourceID"
GROUP BY "ResourceType", "DateTime"
LIMIT 10
WHERE "DateTime">='2019-03-10T00:00' AND "DateTime"<='2019-03-10T05:00'
'''.format('\n            ,'.join([f'(\'{r[0][1]},{r[0][2]}\')' for r in resource_ids.iterrows()]))

print(s)

