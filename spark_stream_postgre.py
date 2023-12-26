import logging
import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import multiprocessing

# ============ POSTGRE FORMULAS ============
def create_postgre_connection():
    # PostgresDB infomation
    postgre_info = {
    'host':'postgres',
    'port':5432,
    'user':'airflow',
    'password':'airflow',
    'database':'airflow'
    }

    try:
        # Connecting to PostgresDB
        conn = psycopg2.connect(**postgre_info)
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        logging.error(f"Could not create postgre connection due to {e}")
        return None

def create_postgre_table(conn, cur):
    # Create schema    
    cur.execute("CREATE SCHEMA IF NOT EXISTS daily_report")
    print("Schema created successfully!")

    # Create table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_report.daily_pu_rev (
        ts_date DATE PRIMARY KEY,
        daily_pu INTEGER,
        daily_rev FLOAT,
        daily_profit FLOAT)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_report.daily_category_product (
        ts_date DATE,
        category TEXT,
        sub_category TEXT,
        product_name TEXT,
        daily_quantity INTEGER,
        daily_rev FLOAT,
        daily_profit FLOAT,
        PRIMARY KEY (ts_date, category, sub_category, product_name))
    """)
        
    cur.execute("""
    CREATE TABLE IF NOT EXISTS daily_report.daily_address (
        ts_date DATE,
        latitude TEXT,
        longitude TEXT,
        daily_pu INTEGER,
        PRIMARY KEY (ts_date, latitude, longitude))
    """)
    
    conn.commit()
    print("Postgre tables created successfully!")
    

## Consume to PostgreDB
def consume_kafka_daily_pu_rev_to_postgres(conn, cur):
    conf = {
    'bootstrap.servers': 'broker:29092',
    }   

    consumer = Consumer(conf | {
    'group.id': 'daily_report_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
    })
    
    consumer.subscribe(['daily_pu_rev'])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        else:
            record = json.loads(msg.value().decode('utf-8'))
            
            cur.execute(f"""
                INSERT INTO daily_report.daily_pu_rev (ts_date, daily_pu, daily_rev, daily_profit)
                VALUES (%s, %s, %s, %s) ON CONFLICT (ts_date) DO UPDATE
                SET daily_pu = EXCLUDED.daily_pu,
                    daily_rev = EXCLUDED.daily_rev,
                    daily_profit = EXCLUDED.daily_profit
                    """
            , (record['ts_date'], record['daily_pu'], record['daily_rev'] ,record['daily_profit']))

            conn.commit()

def consume_kafka_daily_category_product_to_postgres(conn, cur):
    conf = {
    'bootstrap.servers': 'broker:29092',
    }   

    consumer = Consumer(conf | {
    'group.id': 'daily_report_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
    })
    
    consumer.subscribe(['daily_category_product'])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        else:
            record = json.loads(msg.value().decode('utf-8'))
            
            cur.execute(f"""
                INSERT INTO daily_report.daily_category_product (ts_date, category, sub_category, product_name, daily_quantity, daily_rev, daily_profit)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (ts_date, category, sub_category, product_name) DO UPDATE
                SET daily_quantity = EXCLUDED.daily_quantity,
                    daily_rev = EXCLUDED.daily_rev,
                    daily_profit = EXCLUDED.daily_profit
                    """
            , (record['ts_date'], record['category'], record['sub_category'], record['product_name'], record['daily_quantity'], record['daily_rev'] ,record['daily_profit']))

            conn.commit()

def consume_kafka_daily_address_to_postgres(conn, cur):
    conf = {
    'bootstrap.servers': 'broker:29092',
    }   

    consumer = Consumer(conf | {
    'group.id': 'daily_report_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
    })
    
    consumer.subscribe(['daily_address'])

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        else:
            record = json.loads(msg.value().decode('utf-8'))
            
            cur.execute(f"""
                INSERT INTO daily_report.daily_address (ts_date, latitude, longitude, daily_pu)
                VALUES (%s, %s, %s, %s) ON CONFLICT (ts_date, latitude, longitude) DO UPDATE
                SET daily_pu = EXCLUDED.daily_pu
                    """
            , (record['ts_date'], record['latitude'], record['longitude'], record['daily_pu']))

            conn.commit()



if __name__=="__main__":
    
    # Streaming from Kakfa to PostgreDB
    # Connect to PostgreDB
    conn, cur = create_postgre_connection()

    ## Create tables in PostGreDB
    create_postgre_table(conn, cur)
    
    ## Sink data from Kafka to PostgreDB
    daily_pu_rev_to_postgres = multiprocessing.Process(target=consume_kafka_daily_pu_rev_to_postgres, args=(conn, cur))
    daily_category_product_to_postgres = multiprocessing.Process(target=consume_kafka_daily_category_product_to_postgres, args=(conn, cur))
    daily_address_to_postgres = multiprocessing.Process(target=consume_kafka_daily_address_to_postgres, args=(conn, cur))


    # Start processing
    daily_pu_rev_to_postgres.start()
    daily_category_product_to_postgres.start()
    daily_address_to_postgres.start()

    # Wait processes finished
    daily_pu_rev_to_postgres.join()
    daily_category_product_to_postgres.join()
    daily_address_to_postgres.join()

