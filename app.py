import pandas as pd
import glob
import sqlite3
import numpy as np

tmpfile    = "dealership_temp.tmp"               # store all extracted data

logfile    = "dealership_logfile.txt"            # all event logs will be stored

targetfile = "dealership_transformed_data.csv"   # transformed data is stored


conn = sqlite3.connect("data_warehouse.sqlite3")

def extract_from_sql(file_to_process):
    conn1 = sqlite3.connect(file_to_process)
    df = pd.read_sql("SELECT * FROM my_table", conn1)
    df.reset_index(drop=True, inplace=True)
    return df

def extract_from_csv(file_to_process): 
    dataframe = pd.read_csv(file_to_process) 
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process,lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    dataframe = pd.read_xml(file_to_process)
    return dataframe

files = glob.glob("dealership_data"+ "/*")

print(files)
result = pd.DataFrame()

for data in files:
    if ".csv" in data:
        df = extract_from_csv(data)
        result = result.append(df)

    elif ".xml" in data:
        df = extract_from_xml(data)
        result = result.append(df)

    elif ".json" in data:
        df = extract_from_json(data)
        result = result.append(df)

    elif ".sqlite" in data:
        df = extract_from_sql(data)
        df = df.reset_index(drop=True)
        result = result.append(df)

    print()

del result["index"]
result = result.reset_index(level=None,drop=True,inplace=False,col_level=0,col_fill='')
result.to_sql("my_table", conn, if_exists="replace")