from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("FlightsDataAnalysis")\
        .getOrCreate()

    #Read flights_data.csv file
    flights_data_path = "../data/flights_data.csv"
    df = spark.read.csv(flights_data_path, header=True, inferSchema=True)

    #Register a temporary SQL table
    df.createOrReplaceTempView("flights_data_table")

    #Show content of the flights data DataFrame
    spark.sql("SELECT * FROM flights_data_table").show(df.count(), truncate=False)

    #Show number of occurrences
    spark.sql("SELECT COUNT(*) AS num_occurrences FROM flights_data_table").show()

    #Show table schema
    spark.sql("DESCRIBE flights_data_table").show()

    #Drop rows with missing values
    df_no_missing = spark.sql("SELECT * FROM flights_data_table WHERE " + " AND ".join([f"{col} IS NOT NULL" for col in df.columns]))

    #Show content after dropping missing values
    df_no_missing.show(df_no_missing.count(), truncate=False)

    #Show number of occurrences after dropping missing value
    spark.sql("SELECT COUNT(*) AS num_occurrences FROM flights_data_table WHERE " + " AND ".join([f"{col} IS NOT NULL" for col in df.columns])).show()
    
    #Continuing from Q3a using the same .py file
    #No of flights per year, month – from highest to lowest count
    flights_per_year_month = spark.sql("""
        SELECT year, month, COUNT(*) AS num_flights
        FROM flights_data_table
        GROUP BY year, month
        ORDER BY num_flights DESC""")
    flights_per_year_month.show()

    #No of flights per day – from highest to lowest count
    flights_per_day = spark.sql("""
        SELECT year, month, day, COUNT(*) AS num_flights
        FROM flights_data_table
        GROUP BY year, month, day
        ORDER BY num_flights DESC""")
    flights_per_day.show()

    #No of flights per carrier in % — from highest to lowest count
    flights_per_carrier = spark.sql("""
        SELECT carrier, COUNT(*) AS num_flights, (COUNT(*) / (SELECT COUNT(*) FROM flights_data_table)) * 100 AS percentage
        FROM flights_data_table
        GROUP BY carrier
        ORDER BY num_flights DESC""")
    flights_per_carrier.show()

    #No of trips each origin airport — from highest to lowest count
    origin_airports = spark.sql("""
        SELECT origin, COUNT(*) AS num_trips
        FROM flights_data_table
        GROUP BY origin
        ORDER BY num_trips DESC""")
    origin_airports.show()

    #Same for destination airport
    destination_airports = spark.sql("""
        SELECT dest, COUNT(*) AS num_trips
        FROM flights_data_table
        GROUP BY dest
        ORDER BY num_trips DESC""")
    destination_airports.show()

    #10 planes with most flights using tailnum — from highest to lowest count
    top10_planes = spark.sql("""
        SELECT tailnum, COUNT(*) AS num_flights
        FROM flights_data_table
        GROUP BY tailnum
        ORDER BY num_flights DESC LIMIT 10""")
    top10_planes.show()

    #Continuing from Q3b using the same .py file
    #No of flights departure per hour — from highest to lowest
    no_of_depart_flight_per_hr = spark.sql("""
        SELECT hour, COUNT(*) AS num__depart_flights
        FROM flights_data_table
        WHERE dep_time IS NOT NULL
        GROUP BY hour
        ORDER BY num__depart_flights DESC""")
    no_of_depart_flight_per_hr.show()

    #Avg (+) departure delay per carrier – high to low result
    avg_positive_depart_delay_per_carrier = spark.sql("""
        SELECT carrier, AVG(dep_delay) AS avg_positive_depart_delay
        FROM flights_data_table
        WHERE dep_delay > 0
        GROUP BY carrier
        ORDER BY avg_positive_depart_delay DESC""")
    avg_positive_depart_delay_per_carrier.show()

    #Avg (+) departure delay
    avg_positive_departure_delay = spark.sql("""
        SELECT AVG(dep_delay) AS avg_positive_depart_delay
        FROM flights_data_table
        WHERE dep_delay > 0""")
    avg_positive_departure_delay.show()

    #Avg (+) departure delay per month – high to low result
    avg_positive_depart_delay_per_month = spark.sql("""
        SELECT year, month, AVG(dep_delay) AS avg_positive_depart_delay_per_mth
        FROM flights_data_table
        WHERE dep_delay > 0
        GROUP BY year, month
        ORDER BY avg_positive_depart_delay_per_mth DESC""")
    avg_positive_depart_delay_per_month.show()

    #Avg (+) departure delay per hour – high to low result
    avg_positive_depart_delay_per_hour = spark.sql("""
        SELECT hour, AVG(dep_delay) AS avg_positive_depart_delay_per_hr
        FROM flights_data_table
        WHERE dep_delay > 0
        GROUP BY hour
        ORDER BY avg_positive_depart_delay_per_hr DESC""")
    avg_positive_depart_delay_per_hour.show()

    #Avg (-) departure delay per carrier – high to low result
    avg_negative_depart_delay_per_carrier = spark.sql("""
        SELECT carrier, AVG(dep_delay) AS avg_negative_depart_delay
        FROM flights_data_table
        WHERE dep_delay < 0
        GROUP BY carrier
        ORDER BY avg_negative_depart_delay DESC""")
    avg_negative_depart_delay_per_carrier.show()

    #Avg (-) departure delay
    avg_negative_departure_delay = spark.sql("""
        SELECT AVG(dep_delay) AS avg_negative_depart_delay
        FROM flights_data_table
        WHERE dep_delay < 0""")
    avg_negative_departure_delay.show()

    #Avg (-) departure delay per month – high to low result
    avg_negative_depart_delay_per_month = spark.sql("""
        SELECT year, month, AVG(dep_delay) AS avg_negative_depart_delay_per_mth
        FROM flights_data_table
        WHERE dep_delay < 0
        GROUP BY year, month
        ORDER BY avg_negative_depart_delay_per_mth DESC""")
    avg_negative_depart_delay_per_month.show()

    #Avg (-) departure delay per hour – high to low result
    avg_negative_depart_delay_per_hour = spark.sql("""
        SELECT hour, AVG(dep_delay) AS avg_negative_depart_delay_per_hr
        FROM flights_data_table
        WHERE dep_delay < 0
        GROUP BY hour
        ORDER BY avg_negative_depart_delay_per_hr DESC""")
    avg_negative_depart_delay_per_hour.show()

    spark.stop()
