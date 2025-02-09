# Integrated Digital Twin for Weather Station Sensor Data Processing

This project showcases my solution for centralising data from multiple sensor types (humidity, rain, temperature, wind, and others) into a unified "digital twin". The dataset comes from the City of Chicago's automated weather stations, and the goal is to process and enrich the data for real-time analysis.

  
## Objectives:
- **Understand the dataset** and analyse sensor data with a data scientist mindset.

- **Exposure to real-world dataset analysis** from IoT sensors.

- **Design and implement computation logic** in Python for ETL operations.

- **Leverage Pandas DataFrames** for data extraction, transformation, and calculation operations.

- **Visualise data** effectively to showcase insights.

- **Structure code** using methods, loops, and conditions for maintainability.

  
## Dataset Files:
A record in each CSV file can be identified uniquely by (`Station Name`, `Measurement Timestamp`).
| **Filename**       | **Description**                                                                                                                            | **Headers**                                                                                                                                                       |
|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `humidity.csv`     | Contains humidity sensor readings from various stations.                                                                                   | • Humidity                                                                                                                                                        |
| `rain.csv`         | Contains rain-related sensor data, including intensity and accumulation.                                                                   | • Rain Intensity, Interval Rain, Total Rain                                                                                                          |
| `temperature.csv`  | Contains temperature readings, including wet bulb temperature.                                                                             | • Air Temperature, Wet Bulb Temperature                                                                                                                     |
| `wind.csv`         | Contains wind speed, direction, and related measurements.                                                                                  | • Wind Direction, Wind Speed, Maximum Wind Speed, Heading                                                                                       |
| `others.csv`       | Contains additional environmental sensor data.                                                                                             | • Precipitation Type, Barometric Pressure, Solar Radiation, Battery Life                                                                        |

  
## Tasks:

### a) Loading and Combining Sensor Data
1. **Step 1**: Add a new column "Sensor Type" for each sensor dataset (humidity, rain, temperature, wind, others) indicating its respective sensor type.
2. **Step 2**: Combine all five datasets into a single dataframe and sort it by **Measurement Timestamp**.



### b) Enriching the Digital Twin
Enrich the dataframe to represent the digital twin by updating each row with the latest sensor values available for that station name and timestamp. For example:

- For a given `Station Name` and `Measurement Timestamp`, we update each sensor column with the latest value for that station up until the timestamp.



### c) Creating Hourly Data
1. Break the dataset into 1-hour intervals, considering a maximum delay of **15 minutes 59 seconds**.
2. For records within each interval, keep only the latest record to represent the data for that period.



### d) Visualising Air Temperature Trends
Plot the air temperature for each station along with the hourly timestamps. Insights:

1. Temperature patterns for different stations.
2. Comparison of hourly temperature trends across stations.

![Output of Task d](assets/Output%20of%20Task%20d.png)

### e) Correlation Between Humidity and Rain Intensity
Visualise the correlation between humidity and rain intensity during rainy periods. Insights:

1. How humidity varies with rain intensity.
2. The relationship between higher humidity and stronger rain intensity.

![Output of Task e](assets/Output%20of%20Task%20e.png)

  
## Project Structure:
```
/root
├── /data
│   ├── humidity.csv
│   ├── rain.csv
│   ├── temperature.csv
│   ├── wind.csv
│   └── others.csv
├── /src
│   ├── data_processing.py
└── /output
    ├── enriched_data.csv
    ├── hourly_data.csv
    └── air_temperature_plot.png

```

   
## Requirements:
You can install the necessary dependencies using pip:
```bash
pip install pandas matplotlib seaborn
```

 