from datetime import datetime, timedelta
import pandas as pd
import time
import random
from geopy.geocoders import Nominatim
from retrying import retry
import psycopg2
from sqlalchemy import create_engine
import json
from confluent_kafka import SerializingProducer
import uuid

## Kafka settings
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

## Postgres settings
def create_table(conn, cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS store_record (
            ts_id TEXT PRIMARY KEY,
            ts TIMESTAMP,
            customer_id VARCHAR(255),
            customer_name VARCHAR(255),
            segment VARCHAR(255),
            country VARCHAR(255),
            city VARCHAR(255),
            category VARCHAR(255),
            sub_category VARCHAR(255),
            price FLOAT,
            quantity INTEGER,
            revenue FLOAT,
            lat_long point                
        )
    """)
    conn.commit()

def insert_record_postgre(conn, cur, data):
    cur.execute('''
        INSERT INTO store_record (ts_id, ts, customer_id, customer_name, segment, country, city, category, sub_category, price, quantity, revenue, lat_long)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ''', (data['ts_id'], data['ts'], data['customer_id'], data['customer_name'], data['segment'], data['country'],
          data['city'], data['category'], data['sub_category'], data['price'], data['quantity'], data['revenue'], data['lat_long']))
    conn.commit()

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
def generate_date_time():
    day = random.randint(1, 31)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    timestamp = datetime(2023, 12, day, hour, minute, second)

    return timestamp.strftime('%Y-%m-%d %H:%M:%S')

### Navicate the latitude ang longitude
@retry(wait_exponential_multiplier=1000, wait_exponential_max=10000)
def get_lat_long(city, country):
    geolocator = Nominatim(user_agent='hauct_geopy_key')
    location = geolocator.geocode(f'{city}, {country}', timeout=10)

    if location:
        lat_long = f"({location.latitude}, {location.longitude})"
        return lat_long

    return None

### Generate records
def generate_log():
    customer_info_df = extract_customer_info(sample_df)
    product_info_df = extract_product_info(sample_df)
    discount_list = [0.5, 0.4, 0.3, 0]
    
    customer_info_dict = generate_customer_info(customer_info_df)
    product_info_dict = generate_product_info(product_info_df)
    quantity = random.randint(1,20)
    discount = random.choices(discount_list, weights=(0.5,0.15,0.2,0.6))[0]
    ts = generate_date_time()
    
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

if __name__=='__main__':
    sample_df = pd.read_csv('sample_superstore.csv')
    
    conn = psycopg2.connect("host=localhost dbname=store user=postgres password=postgres")
    cur = conn.cursor()

    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092', })
    store_topic = 'store'

    create_table(conn, cur)
    
    while True:
        for i in range(10):
            data = generate_log()
            
            # Ingest data to postgre database
            insert_record_postgre(conn, cur, data)
            print(f'Sent data to postgres database: {data}')

            # produce data to Kakfa
            producer.produce(
                store_topic,
                key=data["ts_id"],
                value=json.dumps(data),
                on_delivery=delivery_report
            )
            print('Produced voter {}, data: {}'.format(i, data))
            producer.flush()