import sys
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning


def run_data():

    db_connector = DatabaseConnector('/Users/zhe/Documents/Learn/aicore/proj2/pro2/db_creds.yaml')
    data_extractor = DataExtractor()
    table_name = 'legacy_users'
    tables = data_extractor.read_rds_table(db_connector, table_name)


    cleaner = DataCleaning()
    df = cleaner.clean_user_data(tables)
    db_connector.upload_to_db(df, 'dim_user')

if __name__ == '__main__':
    run_data()
