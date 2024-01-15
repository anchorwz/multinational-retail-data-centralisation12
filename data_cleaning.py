import pandas as pd
import numpy as np
from datetime import date
import string
from unidecode import unidecode




class DataCleaning:
    """
        Clean data from each of the data sources
    """


    def clean_user_data(self, df):
        """
            Cleaning data
        """

        df = self.remove_nulls(df)
        df = self.clean_dates(df)
        df = self.clean_names(df, 'first_name', 'last_name')
        df = self.clean_phones(df, 'phone_number')

        df = self.remove_nulls(df)

        return df


    def remove_nulls(self, df):
        """
            Remove nulls from dataframe
        """

        df.dropna(inplace=True)
        for column in list(df.columns.values):
            df = df[~(df[column].apply(self.is_null_str))]

        return df


    def is_null_str(self, var):
        """
            Check if string is null
        """
        var_str =  str(var)
        var_str_lowercase = var_str.lower()

        list_null_str = ['null', 'none', 'n/a', 'nan']

        if var_str_lowercase in list_null_str:
            return True
        else:
            return False



    def clean_dates(self, df):
        """
            Clean usr dates information
        """

        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors='coerce').dt.date  
        df['join_date'] = pd.to_datetime(df['join_date'], errors='coerce').dt.date

        join_date_before_birth = df['join_date'] < df['date_of_birth']
        df = df[~join_date_before_birth]

        current_date = date.today()
        birth_date_after_today = df['date_of_birth'] > current_date
        join_date_after_today = df['join_date'] > current_date
        df = df[~birth_date_after_today]
        df = df[~join_date_after_today]

        return df


    def clean_names(self, input_df, *columns_to_clean):
        """
            Clean user names
        """
        for col in columns_to_clean:
            input_df = input_df[input_df[col].apply(self.is_valid_name)]

        return input_df


    def is_valid_name(self, name_input):
        """
            Check if the input name is valid or not
        """

        valid_name_chars = list(string.ascii_lowercase) + list(string.ascii_uppercase) + list(['-', ' '])
        
        name_without_accents = unidecode(name_input)
        for char in name_without_accents:
            if char not in valid_name_chars:
                return False
        return True


    def clean_phones(self, df, column_name):
        # Remove rows with wrong phone_number formatting
        regex_expression = '^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
        df.loc[~df[column_name].str.match(regex_expression), 'phone_number'] = np.nan
        df.dropna(inplace=True)

        return df
        

