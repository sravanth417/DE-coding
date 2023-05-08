import csv
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert_to_parquet():
    csv_file = 'AAAU.csv'
    parquet_file = 'AAAU.parquet'

    df = pd.read_csv(csv_file)

    # Convert the DataFrame to a PyArrow Table
    table = pa.Table.from_pandas(df)

    # Write the PyArrow Table to a Parquet file
    pq.write_table(table, parquet_file)

def get_csv_files():
    for filename in os.listdir("."):
        if filename.endswith(".csv"):
            print(filename.split(".")[0])
            file_path = os.path.join("", filename)
            df = pd.read_csv(file_path)
            print(df.columns)

def find_in_file():
    for filename in os.listdir("archive"):
        if filename.endswith(".csv"):
            file_path = os.path.join("archive", filename)
            df = pd.read_csv(file_path)

            target_row = df.loc[df['Symbol'] == "AAAU"]
            print(target_row["Security Name"])

def add_column_to_csv():
    for filename in os.listdir("."):
        if filename.endswith(".csv"):
            file_path = os.path.join("", filename)
            df = pd.read_csv(file_path)
            df1 = pd.read_csv("archive/symbols_valid_meta.csv")

            df["Symbol"] = filename.split(".")[0]
            df["Security Name"] = df1.loc[df1['Symbol'] == filename.split(".")[0]]["Security Name"].iloc[0]

            print(df.to_csv(filename, index=False))

# get_csv_files()
# find_in_file()

# add_column_to_csv()
# convert_to_parquet()

def rolling_volume_moving_average():
    df = pd.read_csv("AAAU.csv")
    df['vol_moving_avg'] = df['Volume'].rolling(window=30, min_periods=1).mean()
    print(df)

    df.to_csv("AAAU.csv", index=False)

def rolling_adj_close_median():
    df = pd.read_csv("merged.csv")
    df['adj_close_rolling_med'] = df.groupby("Symbol")['Adj Close'].rolling(window=30, min_periods=1).median().reset_index(drop=True)
    
    df.to_csv("merged.csv", index=False)
# rolling_adj_close_median()


def merge_all_csv():
    df = pd.DataFrame()
    for filename in os.listdir("archive/etfs"):
        if filename.endswith(".csv"):
            filepath = os.path.join("archive/etfs", filename)
            print(filepath)
            df = pd.concat([df, pd.read_csv(filepath)])

    df.to_csv("merged.csv", index=False)
    
    print(df)
# merge_all_csv()

def get_length():
    sc = []
    # for filename in os.listdir("archive/etfs"):
    #     sc.append(filename)
    # for filename in os.listdir("archive/stocks"):
    #     sc.append(filename)
    # print(len(sc))

    df = pd.read_csv("archive/symbols_valid_meta.csv")
    for filename in os.listdir("archive/etfs"):
        security_name_val = df.loc[df['NASDAQ Symbol'] == filename.split(".csv")[0]]["Security Name"].iloc[0]
        sc.append(security_name_val)
    for filename in os.listdir("archive/stocks"):
        print(filename)
        security_name_val = df.loc[df['NASDAQ Symbol'] == filename.split(".csv")[0]]["Security Name"].iloc[0]
        sc.append(security_name_val)

    print(len(sc))

# get_length()