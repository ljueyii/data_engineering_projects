# PySpark Flight Data Analysis

This project demonstrates the use of PySpark DataFrame APIs to analyze flight and aircraft data. The objective is to perform data preprocessing, exploratory analysis, and advanced aggregations using Spark.
 
## Objectives:
- **Load and clean data** by reading CSV files into Spark DataFrames.

- **Perform data exploration** using descriptive statistics and aggregations

- **Analyse flight patterns**, delays, and aircraft efficiency.

- **Integrate multiple datasets** using joins and transformations.

- **Use PySpark SQL** to derive insights from structured data.


 
## Tasks:

### a) Data Loading and Cleaning
1. Read `flights_data.csv` into a Spark DataFrame and display content, occurrences, and schema.
2. Identify and drop rows with missing values, displaying the cleaned dataset.



### b) Basic Flight Data Analysis
1. Count the number of flights per **year**, **month**, sorted in descending order.
2. Count the number of flights per **day**, sorted in descending order.
3. Compute the number and percentage of flights per **carrier**, sorted in descending order.
4. Identify the **origin airports** and their trip counts, then do the same for **destination airports**.
5. Find the **Top 10 planes** (by `tailnum`) that made the most flights, sorted in descending order.



### c) Departure Analysis and Delay Investigation
1. Count flight **departures per hour**, sorted in descending order.
2. Compute the **average positive departure delay per carrier**, sorted in descending order.
3. Compute the **average departure delay per month**, sorted in descending order.
4. Compute the **average departure delay per hour**, sorted in descending order.
5. Repeat the above analysis for **negative departure delays**.



### d) Travel Distance, Speed, and Flight Duration Analysis
1. Find the **average, minimum, and maximum flight distance per carrier**, sorted by average distance.
2. Create and compute a new column `flight_speed (miles per hour)`.  *Note that flight air time is in minutes.*
3. Find the **average, minimum, and maximum flight speed per carrier**, sorted by average speed.
4. Find the **shortest flight from PDX** by *distance* and **longest flight from SEA** by *duration*.
5. Compute the **average and total flight duration of carrier "UA"** for flights originating from **SEA**.



### e) Combining Flight Data with Plane Information
1. Read `planes_data.csv` and rename the `year` column to `plane_year`, dropping `speed`.
2. Perform an **inner join** between flight and plane datasets on `tailnum`.
3. Drop rows with missing values in the joined dataset and display the cleaned results.
4. Identify the **Top 20 planes** (carrier, model, plane_year) with the most trips, sorted in descending order.
5. Identify the **Bottom 20 planes** with the least trips, sorted in ascending order.
6. Repeat the **Top 20** and **Bottom 20 plane** trip analysis using **PySpark SQL**.



## Requirements:

### Data Sources:
| **Filename**       | **Description**                                                                                                            |
|--------------------|----------------------------------------------------------------------------------------------------------------------------|
| `flights_data.csv` | Contains detailed flight records, including departure/arrival times, delays, origin, destination, and carrier information. |
| `planes_data.csv`  | Provides aircraft details such as tail number, manufacturer, model, year of manufacture, and engine specifications.        |

### Software:
| **Software**        | **Version**  | **Description**                                                   |
|---------------------|--------------|-------------------------------------------------------------------|
| **Apache Spark**    | 3.x or later | Required for running PySpark jobs using Dataframes.                    |
| **Python**          | 3.6 or later | Required for running PySpark scripts.                            |
| **Java**            | 8 or later   | Java is required by Spark for execution.                         |

### Python Libraries:
| **Library**         | **Version**  | **Description**                                                   |
|---------------------|--------------|-------------------------------------------------------------------|
| **PySpark**         | 3.x or later | For performing distributed data processing using DataFrames.            |
| **pyspark.sql**     | 3.x or later | Required for accessing Spark SQL functions and APIs.              |


 
### Installation Instructions:
1. **Install Apache Spark**: Follow the instructions from the [official Spark website](https://spark.apache.org/downloads.html) to download and install Spark.
2. **Install Java**: Download and install [Java 8](https://adoptopenjdk.net/) (or later) if it's not already installed.
3. **Set up the Environment**:
   - Set the `SPARK_HOME` environment variable to your Spark installation directory.
   - Add the `bin/` directory of your Spark installation to your `PATH` environment variable.
4. **Install Python Libraries**:

   Install the required Python libraries using `pip`:  
    *Warning: Ensure that the PySpark version you install matches the Apache Spark version you have installed.*
   ```bash
   pip install pyspark
   ```

### Run the Script
1. Navigate to the directory containing the script:
   ```bash
   cd path/to/script
   ```
2. Execute the script with Python:
   ```bash
   python 'PySpark Flight Data Analysis.py'
   ```