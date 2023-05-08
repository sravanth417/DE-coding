try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from datetime import datetime
    import pandas as pd
    import os
    import pyarrow as pa
    import pyarrow.parquet as pq
    import numpy as np
    from sklearn import model_selection, ensemble, metrics
    import pickle

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def get_security_row_from_meta(security_name):
    """
    Gets security name from meta file.

    Args:
        security_name: Security symbol in the csv.

    Returns:
        Security name corrosponding to symbol
    """

    df = pd.read_csv("/opt/airflow/archive/symbols_valid_meta.csv")
    return df.loc[df['NASDAQ Symbol'] == security_name]["Security Name"].iloc[0]

def run_training(csv_path):
    """
    Runs training for merged dataset and makes the model pickle file.

    Args:
        csv_path: merged csv file path.
    """
    data = pd.read_csv(csv_path)
    data['Date'] = pd.to_datetime(data['Date'])
    data.set_index('Date', inplace=True)

    # Remove rows with NaN values
    data.dropna(inplace=True)

    # Select features and target
    features = ['vol_moving_avg', 'adj_close_rolling_med']
    target = 'Volume'

    X = data[features]
    y = data[target]

    # Split data into train and test sets
    X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, test_size=0.2, random_state=42)

    # Create a RandomForestRegressor model
    model = ensemble.RandomForestRegressor(n_estimators=100, random_state=42)

    # Train the model
    model.fit(X_train, y_train)

    # Make predictions on test data
    y_pred = model.predict(X_test)

    # Calculate the Mean Absolute Error and Mean Squared Error
    mae = metrics.mean_absolute_error(y_test, y_pred)
    mse = metrics.mean_squared_error(y_test, y_pred)

    print(mae, mse)

    # Dump the trained model to a file
    with open('/opt/airflow/archive/finalized_model.pkl', 'wb') as file:
        pickle.dump(model, file)

def process_and_merge_csv_files():
    """
    Dag task to convert etfs and stocks csv in one single merged csv with Symbol and security name.
    """

    df = pd.DataFrame()
    for filename in os.listdir("/opt/airflow/archive/etfs"):
        if filename.endswith(".csv"):
            filepath = os.path.join("/opt/airflow/archive/etfs", filename)
            df1 = pd.read_csv(filepath)

            df1["Security Name"] = get_security_row_from_meta(filename.split(".csv")[0])
            df1["Symbol"] = filename.split(".csv")[0]

            df = pd.concat([df, df1])

    for filename in os.listdir("/opt/airflow/archive/stocks"):
        if filename.endswith(".csv"):
            filepath = os.path.join("/opt/airflow/archive/stocks", filename)
            df1 = pd.read_csv(filepath)

            df1["Security Name"] = get_security_row_from_meta(filename.split(".csv")[0])
            df1["Symbol"] = filename.split(".csv")[0]

            df = pd.concat([df, df1])

    df.to_csv("/opt/airflow/archive/merged.csv", index=False)

def create_parquet_files():
    """
    Creates parquet file of merged csv
    """

    df = pd.read_csv("/opt/airflow/archive/merged.csv")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "/opt/airflow/archive/merged.parquet")

def create_fe():
    """
    Runs Feature engineering for adj_close_rolling_med and vol_moving_avg
    """

    df = pd.read_csv("/opt/airflow/archive/merged.csv")
    df['adj_close_rolling_med'] = df.groupby("Symbol")['Adj Close'].rolling(window=30, min_periods=1).median().reset_index(drop=True)
    df['vol_moving_avg'] = df.groupby("Symbol")['Volume'].rolling(window=30, min_periods=1).mean().reset_index(drop=True)

    df.to_csv("/opt/airflow/archive/merged_fe.csv", index=False)

def create_parquet_fe():
    """
    Creates parquet for Merged feature engineering file.
    """
    
    df = pd.read_csv("/opt/airflow/archive/merged_fe.csv")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "/opt/airflow/archive/merged_fe.parquet")

def train_ml_model():
    """
    Runs ml model for Merged FE csv file.
    """

    csv_path = "/opt/airflow/archive/merged_fe.csv"
    run_training(csv_path)

with DAG(
        dag_id="processing_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "sravanth",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    process_and_merge_csv_files = PythonOperator(
        task_id="process_and_merge_csv_files",
        python_callable=process_and_merge_csv_files
    )

    create_parquet_files = PythonOperator(
        task_id="create_parquet_files",
        python_callable=create_parquet_files
    )
    
    create_fe = PythonOperator(
        task_id="create_fe",
        python_callable=create_fe,
    )

    create_parquet_fe = PythonOperator(
        task_id="create_parquet_fe",
        python_callable=create_parquet_fe
    )

    train_ml_model = PythonOperator(
        task_id="train_ml_model",
        python_callable=train_ml_model
    )

process_and_merge_csv_files >> create_parquet_files >> create_fe >> create_parquet_fe >> train_ml_model