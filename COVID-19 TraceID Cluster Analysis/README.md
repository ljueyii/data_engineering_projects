# COVID-19 TraceID Cluster Analysis

This project showcases my solution for analysing cluster relationships using TraceID. The dataset includes synthetic data about the spread of a virus across a population. It demonstrates my ability to process, transform, and visualise data using Python, along with libraries such as Pandas, Matplotlib, and NetworkX.

 
## Objectives:
- **Understand the dataset** and analyse the virus spread within the population with a data scientist mindset.

- **Design and implement computation routines** in Python, focusing on ETL operations using Python data structures.

- **Leverage Pandas DataFrames** for data extraction, transformation, calculations, and analysis.

- **Structure the code** using methods, loops, and conditions to maintain efficiency and readability.

- **Visualise data** effectively with Matplotlib and NetworkX to showcase insights from the cluster relationships.

 
## Dataset Files:

| **Filename**       | **Description**                                                         |
|--------------------|-------------------------------------------------------------------------|
| `f0_f1.csv`        | Contains f0 to f1 relationship (`trace_id`, `f0`, `f1`).                |
| `f1_f2.csv`        | Contains f1 to f2 relationship.                                         |
| `f2_f3.csv`        | Contains f2 to f3 relationship.                                         |
| `f3_f4.csv`        | Contains f3 to f4 relationship.                                         |
| `f4_f5.csv`        | Contains f4 to f5 relationship.                                         |
| `people.csv`       | Contains information on individuals with fields `person_id` and `name`. |

 
## Tasks:

### a) Number of Unique People per Cluster
The first task is to compute the number of unique people in each cluster, which is represented by a unique trace ID across all 5 datasets.

![Output of Task a](assets/Output%20of%20Task%20a.png)


**b) People Count Visualization**
A histogram visualising the number of people per cluster, followed by an analysis of the output.

![Output of Task b](assets/Output%20of%20Task%20b.png)


### c) People Not in Any Cluster
Identify all people who are not part of any cluster (those with no close contact).

![Output of Task c](assets/Output%20of%20Task%20c.png)


### d) Tracing Paths through a Specified Person
Design a function to find all tracing paths passing through a specified person ID in a given cluster.

![Output of Task d](assets/Output%20of%20Task%20d.png)


### e) Direct and Indirect Relationships
Design a function that checks if there’s a direct or indirect infection path between two people in a given trace ID.

![Output of Task e](assets/Output%20of%20Task%20e.png)


### f) Network Visualization of Tracing Paths
Visualise the tracing paths using NetworkX to show the network of people infected (directly or indirectly) within a specified cluster.

![Output of Task f](assets/Output%20of%20Task%20f.png)


 
## Project Structure:
```
/root
├── /assets
│   ├── Output of Task a.png
│   ├── Output of Task b.png
│   ├── Output of Task c.png
│   ├── Output of Task d.png
│   ├── Output of Task e.png
│   ├── Output of Task f.png
├── /data
│   ├── f0_f1.csv
│   ├── f1_f2.csv
│   ├── f2_f3.csv
│   ├── f3_f4.csv
│   ├── f4_f5.csv
│   └── people.csv
├── /src
│   ├── data_processing.py
│   ├── data_transformation.py
│   ├── tracing_functions.py
│   └── visualization.py
├── requirements.txt
└── README.md
```

   
## Requirements:
You can install the necessary dependencies using pip:
```bash
pip install pandas matplotlib networkx
```

 
