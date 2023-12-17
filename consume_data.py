# # Function to create a Kafka consumer
# def create_kafka_consumer(topic_name):
#     # Set up a Kafka consumer with specified topic and configurations
#     consumer = KafkaConsumer(
#         topic_name,
#         bootstrap_servers='localhost:9092',
#         auto_offset_reset='earliest',
#         value_deserializer=lambda x: json.loads(x.decode('utf-8')))
#     return consumer

# # Function to fetch data from Kafka
# def fetch_data_from_kafka(consumer):
#     # Poll Kafka consumer for messages within a timeout period
#     messages = consumer.poll(timeout_ms=1000)
#     data = []

#     # Extract data from received messages
#     for message in messages.values():
#         data.append(message.value)
#     return data

import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def create_spark_connection():
    spark_conn = None

    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.3.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_conn

def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")
    
def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.store_daily_records (
            ts_id TEXT PRIMARY KEY,
            ts TIMESTAMP,
            customer_id TEXT,
            customer_name TEXT,
            segment TEXT,
            country TEXT,
            city TEXT,
            category TEXT,
            sub_category TEXT,
            price TEXT,
            quantity TEXT,
            revenue TEXT,
            lat_long TEXT)  
    """)

def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'store_daily_records') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("ts_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
        StructField("category", StringType(), False),
        StructField("sub_category", StringType(), False),
        StructField("price", FloatType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("revenue", FloatType(), False),
        StructField("lat_long", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


if __name__=="__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()
        
        if session is not None:
            create_keyspace(session)
            create_table(session)
            
            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'store_daily_records')
                               .start())

            streaming_query.awaitTermination()