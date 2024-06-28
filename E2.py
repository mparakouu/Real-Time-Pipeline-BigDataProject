from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, min
from pyspark.sql.types import *


# spark session
# περιβάλλον εκτέλεσης spark 
spark_session = SparkSession.builder \
    .appName("VehiclesDataFromKafka") \
    .getOrCreate()

#  η δομή των json δεδομένων  
json_schema = StructType() \
    .add("name", StringType()) \
    .add("orig", StringType()) \
    .add("dest", StringType()) \
    .add("time", StringType()) \
    .add("link", StringType()) \
    .add("position", FloatType()) \
    .add("spacing", FloatType()) \
    .add("speed", FloatType())

# δημιουργία ενός dataframe που θα εισάγονται όλα τα δεδομένα 
# DATAFRAME = spark_session.createDataFrame(spark_session.sparkContext.emptyRDD(), json_schema )


# διαβάζει τα json data απο το vehicle_positions
# οριζουμε τη address τους των brokers του kafka cluster -->  "localhost:9092", kafka, spark εκτελούνεται στην ίδια address

dataframe_kafka_raw = spark_session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "150.140.142.71:9092") \
    .option("subscribe", "vehicle_positions") \
    .option("startingOffsets", "earliest") \
    .option("logLevel", "ERROR") \
    .load()



json_to_dataframe_raw = dataframe_kafka_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", json_schema).alias("data")) \
    .select(
        col("data.name").alias("name"),
        col("data.orig").alias("orig"),
        col("data.dest").alias("dest"),
        col("data.link").alias("link"),
        col("data.time").alias("time"),
        col("data.speed").alias("speed")
    )\

def Save_mongodb_raw(batch_df, epoch_id):
    batch_df = batch_df.orderBy("time")
    batch_df.show(truncate=False) 
    batch_df.write \
        .format("mongo") \
        .mode("append") \
        .option("database", "MyVehiclesData") \
        .option("collection", "RawVehiclesData") \
        .option("uri", "mongodb://localhost:27017") \
        .save()
    


# Εκκίνηση της διαδικασίας επεξεργασίας για τα ωμά δεδομένα
selected_raw_DATAFRAME= json_to_dataframe_raw \
    .writeStream \
    .foreachBatch(Save_mongodb_raw) \
    .outputMode("update") \
    .start()




dataframe_kafka = spark_session \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "150.140.142.71:9092") \
    .option("subscribe", "vehicle_positions") \
    .option("startingOffsets", "earliest") \
    .option("logLevel", "ERROR") \
    .load()


# μετατροπή των json data --> dataframe
# κάνουμε ομαδοποίηση , ίδιο time -> ερχονται μαζι -> ιδιο link -> μεση ταχυτητα και αριθμος 
json_to_dataframe = dataframe_kafka.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", json_schema).alias("data")) \
    .select(
        col("data.link").alias("link"),
        col("data.time").alias("time"),
        col("data.speed").alias("speed")
    ) \
    .groupBy("link") \
    .agg(
        count("*").alias("vcount"),
        avg("speed").alias("vspeed"),
        min(col("time")).alias("Time")
    )
    


# --  save τα data --> mongodb
def Save_mongodb(batch_df, epoch_id):
    batch_df = batch_df.orderBy("time")
    batch_df.show(truncate=True) 
    batch_df.write \
        .format("mongo") \
        .mode("append") \
        .option("database", "MyVehiclesData") \
        .option("collection", "ProcessedVehiclesData") \
        .option("uri", "mongodb://localhost:27017") \
        .save()
    


# Εκτύπωση του επιλεγμένου DataFrame
# εκτυπώνεται ολόκληρο το αποτέλεσμα καθε φορά που ενημερώνεται 
# streaming περιβάλλον , το dataframe συνεχώς επεξεργάζεται τα δεδομένα που έρχονται 
selected_DATAFRAME = json_to_dataframe \
    .writeStream \
    .foreachBatch(Save_mongodb) \
    .outputMode("complete") \
    .start()


selected_raw_DATAFRAME.awaitTermination()
selected_DATAFRAME.awaitTermination()