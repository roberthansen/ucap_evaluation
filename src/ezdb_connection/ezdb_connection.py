import pandas as pd
import psycopg2

class EZBDConnection:
    '''
    A class for interacting with CPUC Energy Division's postgres database server
    named "EZDB". Includes methods for executing queries and retrieving results.
    '''
    def __init__(self,login_credentials:dict):
        '''
        Initializes a connection to a postgresql server using the psycopg2
        library with input login credentials.

        parameters:
            login_credentials - A dict object containing a single item with the
                key "pguser", valued with a nested dict with the four following
                items:
                    - db_main - the name of a database on the EZDB server
                    - uid - a valid user name on the EZDB server
                    - passwd - the password associated with the EZDB user
                        account
                    - host - the url of the EZDB primary server or mirror,
                        accessible from the workstation executing this script
        '''
        self.pguser = login_credentials['pguser']
        self.conn = psycopg2.connect(
            database=self.pguser['db_main'],
            user=self.pguser['uid'],
            password=self.pguser['passwd'],
            host=self.pguser['host']
        )

    def execute_query(self,sql_str:str):
        with self.conn.cursor() as curs:
            curs.execute(sql_str)
            column_names = [x[0] for x in curs.description]
            results = pd.DataFrame(curs.fetchall(),columns=column_names)
        return results
    