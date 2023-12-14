from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (SparkSession.builder
            .appName("StoreAnalysis")
            .master("local[*]")  # Use local Spark execution with all available cores
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"\
                    "org.postgresql:postgresql:42.7.1")  # Spark-Kafka integration
            .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
            .getOrCreate())

store_record_schema = StructType([
        StructField("ts_id", StringType(), True),
        StructField("ts", TimestampType(), True),
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("discount", StringType(), True),
        StructField("revenue", StringType(), True),
        StructField("lat_long", StringType(), True)
])

store_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "store") \
    .option("startingOffsets", "earliest") \
    .load()\
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), store_record_schema).alias("data")) \
    .select("data.*")

enriched_store_df = store_df.withWatermark("ts", "1 minute")

def foreach_batch_function(df, epoch_id):
    df.format("jdbc").option("url", f"jdbc:postgresql://postgres:5432/store")\
      .option("dbtable","store_df").option("user","postgres")\
      .option("password", "postgres").save()
  
store_df.writeStream.foreachBatch(foreach_batch_function).start()   

# top5_most_bought_product_name_to_kafka = (top5_most_bought_product_name.selectExpr("to_json(struct(*)) AS value") \
#         .writeStream \
#         .format("kafka") \
#         .option("kafka.bootstrap.servers", "broker:29092") \
#         .option("topic", "top5_most_bought_product_name") \
#         .option("checkpointLocation", "checkpoints/checkpoint1") \
#         .outputMode("update") \
#         .start())