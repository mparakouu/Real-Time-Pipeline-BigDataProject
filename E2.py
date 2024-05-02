from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count
from pyspark.sql.types import StructType, StringType, IntegerType, FloatType


# spark session
spark_session = SparkSession.builder \
    .appName("VehiclesDataFromKafka") \
    .getOrCreate()

#  schema 
schema_json = StructType() \
    .add("name", StringType()) \
    .add("orig", StringType()) \
    .add("dest", StringType()) \
    .add("time", StringType()) \
    .add("link", StringType()) \
    .add("position", FloatType()) \
    .add("spacing", FloatType()) \
    .add("speed", FloatType())

# δημιουργία ενός dataframe που θα εισάγονται όλα τα δεδομένα 
DATAFRAME = spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(), schema_json )


# διαβάζει τα json data απο το vehicle_positions
dataframe_kafka = spark_session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vehicle_positions") \
    .option("logLevel", "ERROR") \
    .load()


# μετατροπή των json data --> dataframe
# κάνουμε ομαδοποίηση , ίδιο time -> ερχονται μαζι -> ιδιο link -> μεση ταχυτητα και αριθμος 
json_to_dataframe = dataframe_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema_json).alias("data")) \
    .select(
        col("data.link").alias("link"),
        col("data.time").alias("time"),
        col("data.speed").alias("speed")
        
    ) \
    .groupBy("link") \
    .agg(
       count("*").alias("vcount"), 
       avg("speed").alias("vspeed")
    )\



# Εκτύπωση του επιλεγμένου DataFrame
query_selected_fields = json_to_dataframe \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()


query_selected_fields.awaitTermination()
