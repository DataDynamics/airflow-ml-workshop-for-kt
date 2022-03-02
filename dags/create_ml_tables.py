from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.sqlite_operator import SqliteOperator
from airflow.utils.dates import days_ago

TRAINING_TABLE = 'training' #Variable.get("training_table")
PREDICTION_TABLE = 'prediction' #Variable.get("prediction_table")

default_args = {
                "start_date": days_ago(1),
                "email": ["airflow_notification@thisisadummydomain.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": timedelta(minutes=1)
                }

dag = DAG("create_ml_tables",
          description="Create ML tables",
          default_args=default_args,
          # run only once
          schedule_interval= "@once"
          )

with dag:
    create_training_table = SqliteOperator(
        task_id="create_training_table",
        sql=f"""
                CREATE TABLE IF NOT EXISTS {TRAINING_TABLE}(
                timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                mape_test REAL,
                rmse_test REAL,
                days_in_test REAL
                );
            """,
        sqlite_conn_id="sqlite_ml"
        )

    create_prediction_table = SqliteOperator(
        task_id="create_prediction_table",
        sql=f"""
                CREATE TABLE IF NOT EXISTS {PREDICTION_TABLE}(
                date_to_predict DATE,
                run_date DATE,
                yhat REAL,
                yhat_upper REAL,
                yhat_lower REAL
                );
            """,
        sqlite_conn_id="sqlite_ml"
        )

    create_prediction_index = SqliteOperator(
        task_id="create_prediction_index",
        sql=f"""
                CREATE UNIQUE INDEX idx_date_to_predict
                ON {PREDICTION_TABLE} (date_to_predict) --!!! FIXME add a ) parenthesis
                ;
            """,
        sqlite_conn_id="sqlite_ml"
        )

    [create_training_table , create_prediction_table] >> create_prediction_index
