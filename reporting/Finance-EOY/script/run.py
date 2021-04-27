"""
This script produces the Finance EOY reports in CSV format,
and a CSV file containing summary data that is required for
the population of the spreadsheet to be submitted with the reports.

To run:

create a config.py file in this script directory

add the following config:

# redshift connection details
db_config = {
    "host": "",
    "port": ,
    "name": "",
    "user": "",
    "password": ""
}

change the sql_dir in this script to the relative path of the SQL scripts

pip install -r requirements.txt
source venv/bin/activate
create an 'outputs' directory if it doesn't exist already
python run.py

CSV files and summary data appear in outputs directory
"""

import os
import sys
import csv
from sqlalchemy import create_engine
import pandas as pd
# import db_config dict with host, port, user, password, database
from config import db_config
from types import SimpleNamespace
from glob import glob

db = SimpleNamespace(**db_config)

engine = create_engine(f'postgresql://{db.user}:{db.password}@{db.host}:{db.port}/{db.name}')

summary_file_path = 'outputs/summary_file.csv'
summary_df = pd.DataFrame()

sql_dir = '../'

sql_file_paths = glob(f"{sql_dir}/*.sql")

# sql_file_paths = ['../dw-scripts/reporting/Finance-EOY/coeff_elec_bpp.sql']

for sql_file_path in sql_file_paths:
    with open(sql_file_path, 'r') as file:
        
        sql = file.read()
        
        # derive filename and paths
        directory = os.path.dirname(file.name)
        sql_filename = os.path.basename(file.name)
        sql_filename_no_ext = ''.join(sql_filename.split('.')[:-1])

        print(f"starting {sql_filename_no_ext}....", end="", flush="True")

        output_filename = sql_filename_no_ext + '.csv'
        output_path = f'outputs/{output_filename}'

        # run the sql in the database and store in a dataframe
        result_df = pd.read_sql(sql, engine)

        # get summary data from file
        first_row = result_df.iloc[0]
        row_count = first_row['count']
        row_count_inc_heading = row_count + 1
        sum_last_numerical_column = first_row['last_numerical_sum'] 

        # output data to a csv
        result_df.to_csv(output_path, index=False)

        del result_df

        summary_df = summary_df.append(
            pd.DataFrame(
                [[row_count, row_count_inc_heading, sum_last_numerical_column, sql_filename, output_filename]],
                columns=['rows', 'rows_inc_heading', 'sum', 'sql_file', 'output_file']
            )
        )

        print('finished')
    
summary_df.to_csv(summary_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)




    