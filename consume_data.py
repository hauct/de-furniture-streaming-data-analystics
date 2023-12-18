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
        ts_date TEXT,
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
        lat_long TEXT);
    """)
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.number_customer_rev (
        ts_date TEXT PRIMARY KEY,
        number_customer TEXT,
        sum_rev TEXT)
    """)

def connect_to_kafka(spark_conn, topic):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', topic) \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df

def create_selection_df_from_kafka(spark_df, schema):
    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
        
    return sel


if __name__=="__main__":
    # Connect to CassandraDB, create keyspace and tables
    session = create_cassandra_connection()
    create_keyspace(session)
    create_table(session)

    # create spark connection
    spark_conn = create_spark_connection()
    
    #============================================================#
    # Streaming 'store_daily_records' from Kafka to CassandraDB
    ## Connect to kafka with spark connection
    daily_records_topic = 'store_daily_records'
    daily_records_stream = connect_to_kafka(spark_conn, daily_records_topic)
    
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
        StructField("price", FloatType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("revenue", FloatType(), False),
        StructField("lat_long", StringType(), False)
    ])
    daily_records_df = create_selection_df_from_kafka(daily_records_stream, daily_records_schema)
    
    ## Streaming to CassandraDB    
    daily_records_streaming_query = daily_records_df.writeStream.format("org.apache.spark.sql.cassandra")\
                        .option('checkpointLocation', '/tmp/checkpoint')\
                        .option('keyspace', 'spark_streams')\
                        .option('table', daily_records_topic)\
                        .start()

    # #============================================================#
    # # Streaming 'number_customer_rev' to Kafka and to CassandraDB
    # ## Aggregate from daily_records_df   
    # number_customer_rev_df = daily_records_df\
    #                     .groupBy('ts_date')\
    #                     .agg(approx_count_distinct(col('customer_id')).alias('number_customer'),
    #                         sum(col('revenue')).alias('sum_rev'))
    
    # ## Write aggregated data to Kafka topics
    # number_customer_rev_to_kafka = number_customer_rev_df.selectExpr("to_json(struct(*)) AS value")\
    #                 .writeStream.format("kafka")\
    #                 .option('kafka.bootstrap.servers', 'broker:29092')\
    #                 .option('topic', 'number_customer_rev')\
    #                 .option('checkpointLocation', '/tmp/checkpoint1')\
    #                 .outputMode("update")\
    #                 .start()

    # ## Connect to kafka with spark connection
    # number_customer_rev_topic = 'number_customer_rev'
    # number_customer_rev_stream = connect_to_kafka(spark_conn, number_customer_rev_topic)

    # ## Get the Spark dataframe stream
    # number_customer_rev_schema = StructType([
    #             StructField("ts_date", DateType(), False),
    #             StructField("number_customer", StringType(), False),
    #             StructField("sum_rev", StringType(), False)])

    # number_customer_stream_df = number_customer_rev_stream.selectExpr("CAST(value AS STRING)") \
    #                 .select(from_json(col('value'), number_customer_rev_schema).alias('data')).select("data.*")
    
    # ## Streaming to CassandraDB
    # number_customer_rev_to_cassandra = number_customer_stream_df\
    #                 .writeStream.format("org.apache.spark.sql.cassandra")\
    #                 .option('checkpointLocation', '/tmp/checkpoint')\
    #                 .option('keyspace', 'spark_streams')\
    #                 .option('table', number_customer_rev_topic)\
    #                 .start()
                    
    # #============================================================#
    # # Start Streaming
    # logging.info("Streaming is being started...")
    
    daily_records_streaming_query.awaitTermination()
    # number_customer_rev_to_cassandra.awaitTermination()