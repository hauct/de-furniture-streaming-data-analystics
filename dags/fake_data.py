from datetime import datetime, timedelta
import pandas as pd
import time
import random
from geopy.geocoders import Nominatim
import psycopg2
from sqlalchemy import create_engine
import simplejson as json
from confluent_kafka import SerializingProducer
import uuid
import logging
# import pendulum

# from airflow import DAG
# from airflow.operators.python import PythonOperator


# ## Kafka settings
# def delivery_report(err, msg):
#     if err is not None:
#         print(f'Message delivery failed: {err}')
#     else:
#         print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# ## Postgres settings
# def create_table(conn, cur):
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS store_record (
#             ts_id TEXT PRIMARY KEY,
#             ts TIMESTAMP,
#             customer_id VARCHAR(255),
#             customer_name VARCHAR(255),
#             segment VARCHAR(255),
#             country VARCHAR(255),
#             city VARCHAR(255),
#             category VARCHAR(255),
#             sub_category VARCHAR(255),
#             price FLOAT,
#             quantity INTEGER,
#             revenue FLOAT,
#             lat_long point                
#         )
#     """)
#     conn.commit()

# def insert_record_postgre(conn, cur, data):
#     cur.execute('''
#         INSERT INTO store_record (ts_id, ts, customer_id, customer_name, segment, country, city, category, sub_category, price, quantity, revenue, lat_long)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     ''', (data['ts_id'], data['ts'], data['customer_id'], data['customer_name'], data['segment'], data['country'],
#           data['city'], data['category'], data['sub_category'], data['price'], data['quantity'], data['revenue'], data['lat_long']))
#     conn.commit()

## -----------------Get a random customer information---------------------
def extract_customer_info(sample_df):
    customer_info_df = sample_df.loc[:, ["customer_id", "customer_name", 'segment', 'country', 'city']]
    customer_info_df = customer_info_df.drop_duplicates()
    return customer_info_df

def generate_customer_info(customer_info_df):
    n = random.randint(0, len(customer_info_df))
    return customer_info_df.to_dict('records')[n]

## -----------------Get a random product infomation-----------------------
def calculate_price(row):
    return row['sales'] / row['quantity'] / (1 - row['discount'])

def extract_product_info(sample_df):
    product_info = sample_df.loc[:, ["category", 'sub_category', 'product_name', 'sales', 'quantity', 'discount']]
    product_info['price'] = product_info.apply(calculate_price, axis=1)
    product_info = product_info.loc[:, ["category", 'sub_category', 'product_name', 'price']]
    product_info = product_info.drop_duplicates()
    return product_info

def generate_product_info(product_info):
    n = random.randint(0, len(product_info))
    return product_info.to_dict('records')[n]

## -----------------Get random number of records--------------------------
### Generate a random date in December 2023
def generate_date_time(day):
    day = random.randint(1, 31)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    timestamp = datetime(2023, 12, day, hour, minute, second)

    return timestamp.strftime('%Y-%m-%d %H:%M:%S')

### Navicate the latitude ang longitude
def get_lat_long(city, country):
    geolocator = Nominatim(user_agent='hauct_geopy_key')
    location = geolocator.geocode(f'{city}, {country}', timeout=10)

    if location:
        lat_long = f"({location.latitude}, {location.longitude})"
        return lat_long

    return None

### Generate records
def generate_log(day, sample_df):
    customer_info_df = extract_customer_info(sample_df)
    product_info_df = extract_product_info(sample_df)
    discount_list = [0.5, 0.4, 0.3, 0]
    
    customer_info_dict = generate_customer_info(customer_info_df)
    product_info_dict = generate_product_info(product_info_df)
    quantity = random.randint(1,20)
    discount = random.choices(discount_list, weights=(0.5,0.15,0.2,0.6))[0]
    ts = generate_date_time(day)
    
    data = {'ts_id': str(uuid.uuid4()),
            'ts':f'{ts}',
            'customer_id':customer_info_dict['customer_id'],
            'customer_name':customer_info_dict['customer_name'],
            'segment':customer_info_dict['segment'],
            'country':customer_info_dict['country'],
            'city':customer_info_dict['city'],
            'category':product_info_dict['category'],
            'sub_category':product_info_dict['sub_category'],
            'product_name':product_info_dict['product_name'],
            'price':product_info_dict['price'],
            'quantity':f'{quantity}',
            'discount':f'{discount}',
            'revenue': float(product_info_dict['price'])*quantity*(1-discount),
            'lat_long': get_lat_long(customer_info_dict['city'], customer_info_dict['country'])
            }
    return data

def stream_data():
    sample_df = pd.read_csv('./data/sample_superstore.csv')
    producer = SerializingProducer({'bootstrap.servers': 'broker:29092'})
    topic = 'store_daily_records'

    for day in range(1,32):
        for _ in range(random.randint(1,30)):
            try:
                data = generate_log(day, sample_df)
                producer.produce(topic, key=data["ts_id"], value=json.dumps(data).encode('utf-8'))

            except Exception as e:
                logging.error(f'An error occured: {e}')
                continue
            
            print(f'Produced day {day}-12-2023')
            
        print(f'Completed day {day}')

stream_data()
# ### Setting Airflow for automation
# local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# default_args = {
#     'owner': 'hauct',
#     'start_date': datetime(2023, 12, 17, 17, 30, tzinfo=local_tz)
# }

# with DAG('fake_data_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
    
#     streaming_task = PythonOperator(
#         task_id = 'fake_data',
#         python_callable=stream_data
#     )