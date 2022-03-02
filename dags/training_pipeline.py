from datetime import timedelta

from airflow import DAG
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from src.model_toolbox import preprocess_raw_data, split_data, fit_and_save_model, \
    predict_test_wt_arima, measure_accuracy

TRAINING_TABLE = 'training' # Variable.get("training_table")

default_args = {
                "start_date": "2019-8-5",
                "email": ["airflow_notification@thisisadummydomain.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 2,
                "retry_delay": timedelta(minutes=5)
                }

dag = DAG("training_pipeline",
          description="ML Training Pipeline",
          schedule_interval="@monthly",
          default_args=default_args,
          dagrun_timeout=timedelta(minutes=60*10),
          catchup=False
          )

def save_model_accuracy(**kwargs):
    ti = kwargs['ti']
    accuracy = ti.xcom_pull(task_ids='measure_accuracy')

    sql_insert = f"""INSERT INTO {TRAINING_TABLE} 
                            (mape_test, rmse_test, days_in_test)
                     VALUES({accuracy['mape_test']}, 
                            {accuracy['rmse_test']},
                            {accuracy['days_in_test']})
                    ;
                  """
    conn_host = SqliteHook(sqlite_conn_id='sqlite_ml').get_conn()
    conn_host.execute(sql_insert)
    conn_host.commit()


with dag:
    task_1_preprocess = PythonOperator(task_id="preprocess_raw_data",
                                       python_callable=preprocess_raw_data
                                       )

    task_2_split = PythonOperator(task_id="split_data",
                                  python_callable=split_data
                                  )

    task_3_fit_and_save = PythonOperator(task_id="fit_and_save_model",
                                         python_callable=fit_and_save_model
                                         )

    task_4_make_prediction = PythonOperator(task_id="predict_test_wt_arima",
                                            python_callable=predict_test_wt_arima
                                            )

    task_5_accuracy = PythonOperator(task_id="measure_accuracy",
                                     python_callable=measure_accuracy
                                     )

    task_6_save = PythonOperator(task_id="save_model_accuracy",
                                 python_callable=save_model_accuracy,
                                 provide_context= True
                                 )

    task_1_preprocess >> task_2_split >> task_3_fit_and_save >> task_4_make_prediction >> task_5_accuracy >> task_6_save
