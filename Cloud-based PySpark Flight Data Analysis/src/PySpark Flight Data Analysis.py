from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, min, max, sum


if __name__ == "__main__":
    #Create a Spark session
    spark = SparkSession.builder.appName("FlightsDataAnalysis").getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    print("############### START: OUTPUT ################\n")

    #Read flights_data.csv file
    flights_data_path = "../data/flights_data.csv"
    df = spark.read.csv(flights_data_path, header=True, inferSchema=True)

    """
    a) Data Loading and Cleaning
    1. Read flights_data.csv into a Spark DataFrame and display content, occurrences, and schema.
    2. Identify and drop rows with missing values, displaying the cleaned dataset.
    """

    #Show content of the flights data DataFrame
    df.show()

    #Show number of occurrences
    print(f"Number of occurrences: {df.count()}")

    #Show table schema
    print("Table schema:")
    df.printSchema()

    #Drop rows with missing values
    df_no_missing = df.na.drop()

    #Show content after dropping missing values
    df_no_missing.show()

    #Show number of occurrences after dropping missing value
    print(f"Number of occurrences after dropping missing values: {df_no_missing.count()}")

    """
    b) Basic Flight Data Analysis
    1. Count the number of flights per year, month, sorted in descending order.
    2. Count the number of flights per day, sorted in descending order.
    3. Compute the number and percentage of flights per carrier, sorted in descending order.
    4. Identify the origin airports and their trip counts, then do the same for destination airports.
    5. Find the Top 10 planes (by tailnum) that made the most flights, sorted in descending order.
    """

    #No of flights per year, month – from highest to lowest count
    flights_per_year_month = df.groupBy("year", "month").count().orderBy(col("count").desc())
    flights_per_year_month.show()

    #No of flights per day – from highest to lowest count
    flights_per_day = df.groupBy("year", "month", "day").count().orderBy(col("count").desc())
    flights_per_day.show()

    #No of flights per carrier in % — from highest to lowest count
    flights_per_carrier = df.groupBy("carrier").count().withColumn("percentage", (col("count") / df.count()) * 100).orderBy(col("count").desc())
    flights_per_carrier.show()

    #No of trips each origin airport — from highest to lowest count
    origin_airports = df.groupBy("origin").count().orderBy(col("count").desc())
    origin_airports.show()

    #Same for destination airport
    destination_airports = df.groupBy("dest").count().orderBy(col("count").desc())
    destination_airports.show()

    #10 planes with most flights using tailnum — from highest to lowest count
    top10_planes = df.groupBy("tailnum").count().orderBy(col("count").desc()).limit(10)
    top10_planes.show()

    """
    c) Departure Analysis and Delay Investigation
    1. Count flight departures per hour, sorted in descending order.
    2. Compute the average positive departure delay per carrier, sorted in descending order.
    3. Compute the average departure delay per month, sorted in descending order.
    4. Compute the average departure delay per hour, sorted in descending order.
    5. Repeat the above analysis for negative departure delays.
    """

    #No of flights departure per hour — from highest to lowest
    no_of_depart_flight_per_hr = df.filter(col("dep_time").isNotNull()) \
        .groupBy("hour") \
        .agg(count("*").alias("num_depart_flights")) \
        .orderBy(col("num_depart_flights").desc())
    no_of_depart_flight_per_hr.show()

    #Avg (+) departure delay per carrier – high to low result
    avg_positive_depart_delay_per_carrier = df.filter(col("dep_delay") > 0) \
        .groupBy("carrier") \
        .agg(avg("dep_delay").alias("avg_positive_depart_delay")) \
        .orderBy(col("avg_positive_depart_delay").desc())
    avg_positive_depart_delay_per_carrier.show()

    #Avg (+) departure delay
    avg_positive_departure_delay = df.filter(col("dep_delay") > 0) \
        .agg(avg("dep_delay").alias("avg_positive_depart_delay"))
    avg_positive_departure_delay.show()

    #Avg (+) departure delay per month – high to low result
    avg_positive_depart_delay_per_month = df.filter(col("dep_delay") > 0) \
        .groupBy("year", "month") \
        .agg(avg("dep_delay").alias("avg_positive_depart_delay_per_mth")) \
        .orderBy(col("avg_positive_depart_delay_per_mth").desc())
    avg_positive_depart_delay_per_month.show()

    #Avg (+) departure delay per hour – high to low result
    avg_positive_depart_delay_per_hour = df.filter(col("dep_delay") > 0) \
        .groupBy("hour") \
        .agg(avg("dep_delay").alias("avg_positive_depart_delay_per_hr")) \
        .orderBy(col("avg_positive_depart_delay_per_hr").desc())
    avg_positive_depart_delay_per_hour.show()

    #Avg (-) departure delay per carrier – high to low result
    avg_negative_depart_delay_per_carrier = df.filter(col("dep_delay") < 0) \
        .groupBy("carrier") \
        .agg(avg("dep_delay").alias("avg_negative_depart_delay")) \
        .orderBy(col("avg_negative_depart_delay").desc())
    avg_negative_depart_delay_per_carrier.show()

    #Avg (-) departure delay
    avg_negative_departure_delay = df.filter(col("dep_delay") < 0) \
        .agg(avg("dep_delay").alias("avg_negative_depart_delay"))
    avg_negative_departure_delay.show()

    #Avg (-) departure delay per month – high to low result
    avg_negative_depart_delay_per_month = df.filter(col("dep_delay") < 0) \
        .groupBy("year", "month") \
        .agg(avg("dep_delay").alias("avg_negative_depart_delay_per_mth")) \
        .orderBy(col("avg_negative_depart_delay_per_mth").desc())
    avg_negative_depart_delay_per_month.show()

    #Avg (-) departure delay per hour – high to low result
    avg_negative_depart_delay_per_hour = df.filter(col("dep_delay") < 0) \
        .groupBy("hour") \
        .agg(avg("dep_delay").alias("avg_negative_depart_delay_per_hr")) \
        .orderBy(col("avg_negative_depart_delay_per_hr").desc())
    avg_negative_depart_delay_per_hour.show()

    """
    d) Travel Distance, Speed, and Flight Duration Analysis
    1. Find the average, minimum, and maximum flight distance per carrier, sorted by average distance.
    2. Create and compute a new column flight_speed (miles per hour). Note that flight air time is in minutes.
    3. Find the average, minimum, and maximum flight speed per carrier, sorted by average speed.
    4. Find the shortest flight from PDX by distance and longest flight from SEA by duration.
    5. Compute the average and total flight duration of carrier "UA" for flights originating from SEA.
    """

    #avg, min, max flight distance per carrier – sort by avg distance high to low
    avg_min_max_distance_per_carrier = df.groupBy("carrier") \
        .agg(avg("distance").alias("avg_distance"),
             min("distance").alias("min_distance"),
             max("distance").alias("max_distance")) \
        .orderBy(col("avg_distance").desc())
    avg_min_max_distance_per_carrier.show()

    #New column – “flight_speed (miles per hour)”
    df = df.withColumn("flight_speed (miles per hour)", col("distance") / (col("air_time") / 60))
    df.show()

    #avg, min, max flight speed of a given carrier, Sort the results by average speed from highest to lowest
    speed_stats_by_carrier = df.groupBy("carrier") \
        .agg(avg("flight_speed (miles per hour)").alias("avg_speed"),
             min("flight_speed (miles per hour)").alias("min_speed"),
             max("flight_speed (miles per hour)").alias("max_speed")) \
        .orderBy(col("avg_speed").desc())
    speed_stats_by_carrier.show()

    #Shortest flight from “PDX” in terms of distance
    pdx_shortest_distance_flight = df.filter(col("origin") == "PDX") \
        .orderBy(col("distance").asc()) \
        .limit(1) \
        .select("origin", "dest", "distance")
    pdx_shortest_distance_flight.show()

    #Longest flight from “SEA” in terms of flight duration
    sea_longest_duration_flight = df.filter(col("origin") == "SEA") \
        .orderBy(col("air_time").desc()) \
        .limit(1) \
        .select("origin", "dest", "air_time")
    sea_longest_duration_flight.show()

    #Average flight duration of carrier “UA” and originated from “SEA”
    ua_sea_avg_flight_duration = df.filter((col("carrier") == "UA") & (col("origin") == "SEA")) \
        .groupBy("carrier", "origin") \
        .agg(avg("air_time").alias("avg_flight_duration"))
    ua_sea_avg_flight_duration.show()

    #Also, compute the total flight duration in hours
    ua_sea_total_flight_duration = df.filter((col("carrier") == "UA") & (col("origin") == "SEA")) \
        .groupBy("carrier", "origin") \
        .agg((sum("air_time") / 60).alias("total_flight_duration_hours"))
    ua_sea_total_flight_duration.show()

    """
    e) Combining Flight Data with Plane Information
    1. Read planes_data.csv and rename the year column to plane_year, dropping speed.
    2. Perform an inner join between flight and plane datasets on tailnum.
    3. Drop rows with missing values in the joined dataset and display the cleaned results.
    4. Identify the Top 20 planes (carrier, model, plane_year) with the most trips, sorted in descending order.
    5. Identify the Bottom 20 planes with the least trips, sorted in ascending order.
    6. Repeat the Top 20 and Bottom 20 plane trip analysis using PySpark SQL.
    """

    #Read the "plane_data.csv" file
    plane_data_path = "../data/planes_data.csv"  
    plane_df = spark.read.csv(plane_data_path, header=True, inferSchema=True)

    #Show content of the planes data DataFrame
    plane_df.show()

    #Show number of occurrences
    print(f"Number of occurrences: {plane_df.count()}")

    #Show table schema
    print("Table schema:")
    plane_df.printSchema()

    #Delete speed column and change year column name to "plane_year"
    plane_df = plane_df.drop("speed").withColumnRenamed("year", "plane_year")

    #Show the modified column name
    plane_df.show()

    #Perform inner join based on key "tailnum"
    plane_df = plane_df.withColumnRenamed("tailnum", "plane_tailnum")
    joined_df = df.join(plane_df, df["tailnum"] == plane_df["plane_tailnum"], "inner")


    #Drop rows with missing data 
    joined_df = joined_df.dropna()

    #Show the content and the number of occurrences of the joined DataFrame
    joined_df.show()
    print(f"Number of occurrences: {joined_df.count()}")

    #Top 20 planes that made most number of trips – sort highest to lowest
    top_twenty_planes = joined_df.groupBy("carrier", "model", "plane_year").count().orderBy(col("count").desc()).limit(20)
    top_twenty_planes.show()

    #Bottom 20 planes that made least number of trips – sort highest to lowest
    bottom_twenty_planes = joined_df.groupBy("carrier", "model", "plane_year").count().orderBy(col("count")).limit(20)
    bottom_twenty_planes.show()

    #Repeat Top and Bottom 20 in spark.sql
    joined_df.createOrReplaceTempView("joined_data")

    top_twenty_planes_sql = spark.sql("""
        SELECT carrier, model, plane_year, COUNT(*) AS count
        FROM joined_data
        GROUP BY carrier, model, plane_year
        ORDER BY count DESC
        LIMIT 20""")
    print("Top 20 using PySpark SQL approach:")
    top_twenty_planes_sql.show()

    bottom_twenty_planes_sql = spark.sql("""
        SELECT carrier, model, plane_year, COUNT(*) AS count
        FROM joined_data
        GROUP BY carrier, model, plane_year
        ORDER BY count
        LIMIT 20""")
    print("Bottom 20 using PySpark SQL approach:")
    bottom_twenty_planes_sql.show()

    print("############### END: OUTPUT   ################\n")
    
    spark.stop()
