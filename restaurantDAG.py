from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import time
import requests as rq
import csv
from bs4 import BeautifulSoup as bs
import re

CSV_HEADERS = ['Restaurant Name', 'Rating', 'Tags', 'Price Range']
BASE_URL = "https://www.yelp.com/search?find_desc=&find_loc=North+York%2C+Toronto%2C+Ontario&start={}"
USER_AGENT = {"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36"}
NUM = 0
CSV_FILE_PATH = '/opt/airflow/tmp/export_data_restaurants.csv'

#connects to postgres db and copies data from csv to postgres
def write_to_postgres():
    postgres_hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    with open(CSV_FILE_PATH, "r") as file:
        cur.copy_expert(
            '''
            DROP TABLE IF EXISTS yelp.restaurants;

            CREATE TABLE yelp.restaurants
            (
                restaurant_name character varying,
                rating double precision,
                tags character varying,
                price_range character varying
            );

            ALTER TABLE IF EXISTS yelp.restaurants
                OWNER to postgres;

            COPY yelp.restaurants FROM STDIN WITH CSV HEADER DELIMITER AS ',';
            ''',
            file,
        )
    conn.commit()

#helper function for execute_webscraping()
def extract_data(restaurant):

    #get name, rating, and tags
    name = restaurant.find(class_="css-19v1rkv").text
    rating = restaurant.find(class_="css-gutk1c").text
    tags = restaurant.find_all(class_="css-11bijt4")
    for k in range (0, len(tags)):
        tags[k] = tags[k].text

    #get price range, if it exists
    try:
        price_range = restaurant.find(class_=lambda x: x and x.startswith('priceRange__')).text
    except:
        price_range = "Not specified"

    #return data
    return [name, rating, tags, price_range]

#helper function for execute_webscraping()
def get_num_pages():
    #request url
    url = BASE_URL.format(0)
    page = rq.get(url, headers=USER_AGENT)
    soup = bs(page.content, "html.parser")
    #find the element that contains the info for pagination
    pagination_info = soup.find_all('div', class_=lambda value: value and value.startswith("pagination__09f24"))
    print (pagination_info is None)
    #find the number of pages within that element
    pagination_info = pagination_info[0].find(class_="css-1aq64zd").text
    num_pages = pagination_info.split(" ")
    num_pages = int(num_pages[len(num_pages)-1])
    return num_pages

def execute_webscraping():
    page_number = NUM
    #prepare csv for writing
    file = open(CSV_FILE_PATH, 'w', newline='')
    writer = csv.writer(file)
    writer.writerow(CSV_HEADERS)

    num_pages = get_num_pages()
    for i in range(0, num_pages):

        #get content from each page
        url = BASE_URL.format(i*10)

        print("Let me sleep for 120 seconds")
        print("ZZzzzz...")
        time.sleep(120)
        print("Was a nice sleep, now let me continue...")
        page = ''
        while page == '':
            try:
                page = rq.get(url, headers=USER_AGENT)
                break
            except:
                print("Connection refused by the server..")
                print("Let me sleep for 120 seconds")
                print("ZZzzzz...")
                time.sleep(120)
                print("Was a nice sleep, now let me continue...")
                continue
        #page = rq.get(url, headers=USER_AGENT)


        soup = bs(page.content, "html.parser")
        
        #get all restaurants listed on each page
        restaurants = soup.find_all(class_="css-ady4rt")

        #go through each restaurant, excluding the sponsored ones
        #the first eight on each page are sponsored. skip those
        for j in range (8, len(restaurants)-1):
            page_number = page_number + 1
            restaurant = restaurants[j]
            #extract data from each restaurant including name, rating, etc
            restaurant_data = extract_data(restaurant)
            #write data to csv
            writer.writerow(restaurant_data)
            #print data
            print(str(page_number) +")")
            for l in range(0, len(restaurant_data)):
                print(restaurant_data[l])
            print()
    file.close()


default_args = {
    'start_date': datetime(2019, 3, 29, 1),
    'owner': 'Airflow'
}

with DAG(dag_id='restaurantDAG', schedule_interval="0 0 * * *", default_args=default_args, catchup=False) as dag:
    
    # Task 1 checks if site is available
    task_1 = HttpSensor(
        task_id='task_1',
        http_conn_id="yelp_conn",
        endpoint="search?find_desc=&find_loc=North+York%2C+Toronto%2C+Ontario&start=0",
        poke_interval=5, #boolean lambda function is executed every 5s
        timeout=20 #after 20 seconds, the response check will timeout and it will end up in failure
        ) 

    # Task 2 extracts and transforms data into a csv
    task_2 = PythonOperator(task_id='task_2', python_callable=execute_webscraping)

    # Task 3 loads data into Postgres 
    task_3 = PythonOperator(task_id='task_3', python_callable=write_to_postgres)

    #COMMENT OUT LATER
    '''# Task 3 copies the csv to tmp
    task_3 = BashOperator(task_id='task_3', bash_command='ls -l')

    # Task 4 saves the data into Postgres
    task_4 = SQLExecuteQueryOperator(
        task_id='task_4',
        sql="""CREATE TABLE IF NOT EXISTS yelp.restaurants(); 
        ALTER TABLE IF EXISTS yelp.restaurants OWNER to postgres;
        COPY yelp.restaurants
            FROM '/home/arian-mint/new-docker/tmp/export_data_restaurants.csv'
            DELIMITER ','
            CSV HEADER;
        """,
        conn_id="postgres_conn"
    )'''


    task_1 >> task_2 >> task_3