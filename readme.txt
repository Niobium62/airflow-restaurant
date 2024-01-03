*make sure you have a local server of postgres on your machine outside of the docker container, that has the database "airflow" with the schema "yelp"





1. create the necessary folders, and set the user id

mkdir -p ./dags ./logs ./plugins ./config ./tmp
echo -e "AIRFLOW_UID=$(id -u)" > .env

2. move the restaurantDAG file to the folder tmp

mv restaurantDAG.py tmp/restaurantDAG.py

3. initialize airflow.

docker compose up airflow-init

4. start airflow

docker compose up

5. on your browser, go to http://localhost:8080/

6. create an HTTP connection.

ID: yelp_conn
Host: https://www.yelp.com

7. create a postgres connection
ID: postgres_conn
Host: host.docker.internal
Database: airflow
Login: postgres
Password: postgres
Port: 5432

8. unpause the DAG. It could take up to an hour to process the data

9. once the DAGRun is complete, you can stop the docker container

docker compose down
