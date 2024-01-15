import yaml
import pandas as pd
from sqlalchemy import Engine, create_engine, inspect

class DatabaseConnector:
    """
        Connect with upload data to the database
    """
    def __init__(self, credentials_path):
        self.credentials_path = credentials_path
        self.credentials = self.read_db_creds()
        self.eng = self.init_db_engine()
        self.db_tables = {}
    

    def read_db_creds(self):
        """
            Read credentials from yaml file
        """

        with open(self.credentials_path, 'r') as f:
            credentials = yaml.safe_load(f)

        return credentials
        

    def init_db_engine(self):
        """
            Initialise DB with credentials
        """

        host = self.credentials['RDS_HOST']
        port = self.credentials['RDS_PORT']
        db = self.credentials['RDS_DATABASE']
        usr = self.credentials['RDS_USER']
        psword = self.credentials['RDS_PASSWORD']

        eng = create_engine(
                url = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(usr, psword, host, port, db), echo=True, isolation_level='AUTOCOMMIT'
        )

        return eng


    def list_db_tables(self):
        """
            List all tables in the DB
        """

        inspector = inspect(self.eng)
        schemas = inspector.get_schema_names()

        # Get all table names in the each schema
        for schema in schemas:
            tables = inspector.get_table_names(schema=schema)
            for table in range(len(tables)):
                print('Table: {}'.format(tables[table]))

            self.db_tables[schema] = tables
        
        return self.db_tables


    def upload_to_db(self, df, name):
        """
            Upload df to store data in the sales_data db in a table named dim_users
        """

        #try:
        df.to_sql(name=name, con=self.eng, if_exists='replace', index=False)
        print('Successful uploaded dataframe to DB.')
        #except Exception as ex:
        #    print('Unsuccessful upload dataframe to DB.')

        


    

