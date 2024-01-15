import pandas as pd

class DataExtractor:
    """
        Utility class
    """


    def test():
        pass


    def read_rds_table(self, db_connector, table_name):
        """
            Take instance of DB_connector and table names return dataframe
        """

        df_table = pd.read_sql_table(table_name, db_connector.eng)

        return df_table



