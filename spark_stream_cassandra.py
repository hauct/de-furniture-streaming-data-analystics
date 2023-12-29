import logging
from cassandra.cluster import Cluster
import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import multiprocessing

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ============ SPARK FOMULAS ============
def create_spark_connection():
    spark_conn = None

    try:
        spark_conn = SparkSession.builder \
            .appName('CassandraStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector-assembly_2.12:3.3.0,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
            .config('spark.cassandra.connection.host', 'cassandra') \
            .getOrCreate()

        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return spark_conn

# ============ KAFKA FOMULAS ============
def connect_to_kafka(spark_conn, topic):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .option("failOnDataLoss", "false")\
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df, schema):
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

# ============ CASSANDRA FOMULAS ============
def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['cassandra'])

        c_session = cluster.connect()

        return c_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None
    
def create_cassandra_table(c_session):
    # Create keyspace
    c_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")
    
    # Create tables
    c_session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.daily_records (
        ts_id TEXT PRIMARY KEY,
        ts TIMESTAMP,
        ts_date DATE,
        customer_id TEXT,
        customer_name TEXT,
        segment TEXT,
        country TEXT,
        city TEXT,
        category TEXT,
        sub_category TEXT,
        product_name TEXT,
        price FLOAT,
        quantity INT,
        revenue FLOAT,
        profit FLOAT,
        latitude TEXT,
        longitude TEXT);
    """)
    c_session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.daily_pu_rev (
        ts_date TEXT PRIMARY KEY,
        daily_pu TEXT,
        daily_rev TEXT,
        daily_profit TEXT)
    """)
    c_session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.daily_category_product (
        ts_date TEXT,
        category TEXT,
        sub_category TEXT,
        product_name TEXT,
        daily_quantity TEXT,
        daily_rev TEXT,
        daily_profit TEXT,
        PRIMARY KEY ((ts_date, category, sub_category, product_name)))
    """)

    c_session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.daily_address (
        ts_date TEXT,
        latitude TEXT,
        longitude TEXT,
        daily_pu TEXT,
        PRIMARY KEY ((ts_date, latitude, longitude)))
    """)

    print("Cassandra tables created successfully!")

if __name__=="__main__":
    # Create spark connection
    spark_conn = create_spark_connection()
    
    #============================================================
    # Stream 'store_daily_records' from Kafka to CassandraDB
    ## Connect to kafka with spark connection
    daily_records_topic = 'daily_records'
    daily_records_stream = connect_to_kafka(spark_conn, daily_records_topic)

    ## Connect to CassandraDB, create keyspace and tables
    keyspace = 'spark_streams'
    c_session = create_cassandra_connection()
    create_cassandra_table(c_session)

    ## Get the Spark dataframe stream
    daily_records_schema = StructType([
        StructField("ts_id", StringType(), False),
        StructField("ts", TimestampType(), False),
        StructField("ts_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("segment", StringType(), False),
        StructField("country", StringType(), False),
        StructField("city", StringType(), False),
        StructField("category", StringType(), False),
        StructField("sub_category", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("price", FloatType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("revenue", FloatType(), False),
        StructField("profit", FloatType(), False),
        StructField("latitude", StringType(), False),
        StructField("longitude", StringType(), False)
    ])
    daily_records_df = create_selection_df_from_kafka(daily_records_stream, daily_records_schema)\
                        .withWatermark("ts", "1 minute")
    
    ## Stream to CassandraDB    
    daily_records_to_cassandra = daily_records_df.writeStream.format("org.apache.spark.sql.cassandra")\
                        .option('checkpointLocation', '/tmp/checkpoint1')\
                        .option('keyspace', keyspace)\
                        .option('table', daily_records_topic)\
                        .start()

    #============================================================
    # Streaming 'daily_pu_rev' to Kafka and to CassandraDB
    '''
    daily_pu_rev: How many people buy and how much revenue there is per day
    '''
    ## Aggregate from daily_records_df   
    daily_pu_rev = daily_records_df\
                        .groupBy('ts_date')\
                        .agg(approx_count_distinct(col('customer_id')).alias('daily_pu'),
                            sum(col('revenue')).alias('daily_rev'),
                            sum(col('profit')).alias('daily_profit'))
    
    ## Write aggregated data to Kafka topics
    daily_pu_rev_topic = 'daily_pu_rev'
    daily_pu_rev_to_kafka = daily_pu_rev.selectExpr("to_json(struct(*)) AS value")\
                    .writeStream.format("kafka")\
                    .option('kafka.bootstrap.servers', 'broker:29092')\
                    .option('topic', daily_pu_rev_topic)\
                    .option('checkpointLocation', '/tmp/checkpoint2')\
                    .outputMode("update")\
                    .start()

    ## Connect to kafka with spark connection
    daily_pu_rev_stream = connect_to_kafka(spark_conn, daily_pu_rev_topic)

    ## Get the Spark dataframe stream
    daily_pu_rev_schema = StructType([
                StructField("ts_date", DateType(), False),
                StructField("daily_pu", StringType(), False),
                StructField("daily_rev", StringType(), False),
                StructField("daily_profit", StringType(), False)])

    daily_pu_rev_df = create_selection_df_from_kafka(daily_pu_rev_stream, daily_pu_rev_schema)
    
    ## Streaming to CassandraDB
    daily_pu_rev_to_cassandra = daily_pu_rev_df\
                    .writeStream.format("org.apache.spark.sql.cassandra")\
                    .option('keyspace', keyspace)\
                    .option('table', daily_pu_rev_topic)\
                    .option('checkpointLocation', '/tmp/checkpoint3')\
                    .start()

    #============================================================#
    # Streaming 'daily_category_product' to Kafka and to CassandraDB
    '''
    daily_category_product: Number and revenue of product (categorized) per day
    '''
    ## Aggregate from daily_category_product   
    daily_category_product = daily_records_df\
                        .groupBy('ts_date', 'category', 'sub_category', 'product_name')\
                        .agg(sum(col('quantity')).alias('daily_quantity'),
                            sum(col('revenue')).alias('daily_rev'),
                            sum(col('profit')).alias('daily_profit'))

    ## Write aggregated data to Kafka topics
    daily_category_product_topic = 'daily_category_product'
    daily_category_product_to_kafka = daily_category_product.selectExpr("to_json(struct(*)) AS value")\
                    .writeStream.format("kafka")\
                    .option('kafka.bootstrap.servers', 'broker:29092')\
                    .option('topic', daily_category_product_topic)\
                    .option('checkpointLocation', '/tmp/checkpoint4')\
                    .outputMode("update")\
                    .start()

    ## Connect to kafka with spark connection
    daily_category_product_stream = connect_to_kafka(spark_conn, daily_category_product_topic)

    ## Get the Spark dataframe stream
    daily_category_product_schema = StructType([
                StructField("ts_date", DateType(), False),
                StructField("category", StringType(), False),
                StructField("sub_category", StringType(), False),
                StructField("product_name", StringType(), False),
                StructField("daily_quantity", StringType(), False),
                StructField("daily_rev", StringType(), False),
                StructField("daily_profit", StringType(), False)])

    daily_category_product_df = create_selection_df_from_kafka(daily_category_product_stream, daily_category_product_schema)
    
    ## Streaming to CassandraDB
    daily_category_product_to_cassandra = daily_category_product_df\
                    .writeStream.format("org.apache.spark.sql.cassandra")\
                    .option('keyspace', keyspace)\
                    .option('table', daily_category_product_topic)\
                    .option('checkpointLocation', '/tmp/checkpoint5')\
                    .start()

    #============================================================#
    # Streaming 'daily_address' to Kafka and to CassandraDB
    '''
    daily_address: Number of address (latitude and longitude) of paying users
    '''
    ## Aggregate from daily_category_product   
    daily_address = daily_records_df\
                        .groupBy('ts_date', 'latitude', 'longitude')\
                        .agg(approx_count_distinct(col('customer_id')).alias('daily_pu'))

    ## Write aggregated data to Kafka topics
    daily_address_topic = 'daily_address'
    daily_address_to_kafka = daily_address.selectExpr("to_json(struct(*)) AS value")\
                    .writeStream.format("kafka")\
                    .option('kafka.bootstrap.servers', 'broker:29092')\
                    .option('topic', daily_address_topic)\
                    .option('checkpointLocation', '/tmp/checkpoint6')\
                    .outputMode("update")\
                    .start()

    ## Connect to kafka with spark connection
    daily_address_stream = connect_to_kafka(spark_conn, daily_address_topic)

    ## Get the Spark dataframe stream
    daily_address_schema = StructType([
                StructField("ts_date", DateType(), False),
                StructField("latitude", StringType(), False),
                StructField("longitude", StringType(), False),
                StructField("daily_pu", StringType(), False)])

    daily_address_df = create_selection_df_from_kafka(daily_address_stream, daily_address_schema)
    
    ## Streaming to CassandraDB
    daily_address_to_cassandra = daily_address_df\
                    .writeStream.format("org.apache.spark.sql.cassandra")\
                    .option('keyspace', keyspace)\
                    .option('table', daily_address_topic)\
                    .option('checkpointLocation', '/tmp/checkpoint7')\
                    .start()
    #============================================================
    # Start Streaming
    logging.info("Streaming is being started...")
    
    daily_records_to_cassandra.awaitTermination()
    daily_pu_rev_to_cassandra.awaitTermination()
    daily_category_product_to_cassandra.awaitTermination()
    daily_address_to_cassandra.awaitTermination()
    