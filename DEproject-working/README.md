# DEproject

Pre-requisite:
1. Any linux machine, Ex: Ubuntu 20.04
2. System requirement: 8Gb RAM and 10GB Storage

Setup Steps:
1. Download the ETF and stock datasets from the primary dataset available at https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset.
2. Run cmd : sudo apt install docker docker-compose && sudo chmod 666 /var/run/docker.sock
3. Run cmd : echo -e "AIRFLOW_UID=$(id -u)" > .env
4. Run cmd : docker-compose up airflow-init
5. Run cmd : docker-compose up
6. Run the DAG on "http://localhost:8080/dags/processing_dag"

API Setup steps:
1. Run cmd : pip3 install -r requirements.txt
2. Run cmd : python3 main.py
3. Ensure the cmd returns success without any errors.
3. Open "http://127.0.0.1:5050/predict?vol_moving_avg=12345&adj_close_rolling_med=25"
