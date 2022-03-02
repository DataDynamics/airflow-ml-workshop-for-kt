## Docker Airflow Setup [:top:](README.md#table-of-contents)

Launch the Airflow Docker container:
```bash
$ docker run -p 127.0.0.1:8080:8080 -e LOAD_EX=y -e PYTHONPATH="/usr/local/airflow/pyconde2019-airflow-ml-workshop" -v $HOME/pyconde2019-airflow-ml-workshop/requirements.txt:/requirements.txt -v $HOME/pyconde2019-airflow-ml-workshop/:/usr/local/airflow/pyconde2019-airflow-ml-workshop:rw -v $HOME/pyconde2019-airflow-ml-workshop/dags/:/usr/local/airflow/dags:rw puckel/docker-airflow webserver
```

The above command specify:
- `8080:8080`: Airflow is reachable at `localhost:8080`
- `LOAD_EX=y `: load the Airflow examples
- `-v $HOME/pyconde2019-airflow-ml-workshop/requirements.txt:/requirements.txt \`: install the requirements for the workshop exercises
- `-v $HOME/pyconde2019-airflow-ml-workshop/:/usr/local/airflow/pyconde2019-airflow-ml-workshop:rw `: mount the project repository volume
- `-v $HOME/pyconde2019-airflow-ml-workshop/dags/:/usr/local/airflow/dags:rw`: mount the volume that contains the dags (the exercise worflows)
- `puckel/docker-airflow webserver`: run Airflow with `SequentialExecutor`
