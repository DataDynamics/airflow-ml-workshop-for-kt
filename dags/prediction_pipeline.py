from datetime import timedelta, date

from airflow import DAG
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from src.model_toolbox import forecast_wt_arima_for_date

PREDICTION_TABLE = 'prediction'

default_args = {
                "start_date": "2022-01-01",
                "email": ["airflow_notification@thisisadummydomain.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": timedelta(minutes=5)
                }


dag = DAG("prediction_pipeline",
          description="ML Prediction Pipeline",
          # predict every day
          schedule_interval= None,
          default_args=default_args,
          dagrun_timeout=timedelta(minutes=60*4),
          catchup=True
          )

def save_prediction(**kwargs):
    ti = kwargs['ti']
    prediction_dict = ti.xcom_pull(task_ids='run_prediction')
    sql_insert = f"""INSERT OR REPLACE INTO {PREDICTION_TABLE} 
                     (date_to_predict, run_date, yhat, yhat_upper, yhat_lower)
                     VALUES ('{prediction_dict["date_to_predict"]}', 
                             '{prediction_dict["run_date"]}',
                             {prediction_dict["yhat"]}, 
                             {prediction_dict["yhat_upper"]},
                             {prediction_dict["yhat_lower"]}
                            )
                     ;"""
    conn_host = SqliteHook(sqlite_conn_id='sqlite_ml').get_conn()
    conn_host.execute(sql_insert)
    conn_host.commit()
    conn_host.close()


def run_prediction(**kwargs):
    prediction_date = kwargs['ds']
    run_date = date.today()
    result = forecast_wt_arima_for_date(str(prediction_date))
    result['date_to_predict'] = prediction_date
    result['run_date'] = run_date
    return result


with dag:
    run_prediction = PythonOperator(task_id="run_prediction",
                                    python_callable=run_prediction,
                                    provide_context=True
                                    )
    save_prediction = PythonOperator(task_id="save_prediction",
                                     python_callable=save_prediction,
                                     provide_context=True
                                     )

    run_prediction >> save_prediction
