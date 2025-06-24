Step 1: Data Generation
1. Installed and ran UXSIM to simulate vehicle movement.
2. Used the vehicles_to_pandas() function to convert the simulation data into a pandas DataFrame.
3. Saved the DataFrame to a CSV file for later use.

Step 2: Kafka Setup & Data Streaming
1. Set up a Kafka broker and created a topic named vehicle_positions
2. Developed a Python script to:
   a. Convert the simulator data into JSON format.
   b. Add a start timestamp to each data point to simulate real-time arrival.
   c. Use KafkaProducer to publish the JSON data to the vehicle_positions topic.
   d. Manage data delays (e.g., vehicles waiting at the origin node) and ensure synchronized arrival based on time.

Step 3: Kafka Consumer
1. Used KafkaConsumer to receive and verify streamed vehicle data

Step 4: Real-Time Processing with Spark
1. Installed Apache Spark and PySpark.
2. Used Spark Structured Streaming to:
   a. Read the JSON data from the Kafka topic.
   b. Convert the stream into a Spark DataFrame.
   c. Group data by link and calculate:
      - The average vehicle speed (vspeed) per link.
      - The vehicle count (vcount) per link.

Step 5: MongoDB Integration
1. Installed MongoDB and the MongoDB Spark connector.
2. Created a database MyVehiclesData with two collections:
   a. RawVehiclesData for raw (unprocessed) JSON data.
   b. ProcessedVehiclesData for aggregated statistics.
3. Saved each data batch from Spark into MongoDB.
4. Used Jupyter Notebook to print and explore both raw and processed data.
   a. Link with the smallest vehicle count within a specific time window.
   b. Link with the highest average speed within the same period.
   c. Longest trip duration per vehicle:
      - Start = first record for the vehicle.
      - End = record with link = trip_end for the same vehicle.
      - Duration = end_time - start_time.
