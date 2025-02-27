## Airflow Setup

Run Airflow:
```bash
$ docker run -p 127.0.0.1:8080:8080 -e LOAD_EX=y -e PYTHONPATH="/usr/local/airflow/airflow-ml-workshop-for-kt" -v $HOME/airflow-ml-workshop-for-kt/requirements.txt:/requirements.txt -v $HOME/airflow-ml-workshop-for-kt/:/usr/local/airflow/airflow-ml-workshop-for-kt:rw -v $HOME/airflow-ml-workshop-for-kt/dags/:/usr/local/airflow/dags:rw puckel/docker-airflow webserver
```

Parameters:
- `8080:8080`: Airflow is reachable at `localhost:8080`
- `LOAD_EX=y `: load the Airflow examples
- `-v $HOME/airflow-ml-workshop-for-kt/requirements.txt:/requirements.txt \`: install the requirements for the workshop exercises
- `-v $HOME/airflow-ml-workshop-for-kt/:/usr/local/airflow/airflow-ml-workshop-for-kt:rw `: mount the project repository volume
- `-v $HOME/airflow-ml-workshop-for-kt/dags/:/usr/local/airflow/dags:rw`: mount the volume that contains the dags (the exercise worflows)
- `puckel/docker-airflow webserver`: run Airflow with `SequentialExecutor`
