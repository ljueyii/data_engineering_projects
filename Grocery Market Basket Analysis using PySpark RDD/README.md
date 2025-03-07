# Grocery Market Basket Analysis using PySpark RDD


 
This project demonstrates the use of PySpark RDDs to perform a market basket analysis on grocery transaction data. The dataset `grocery_data.csv` represents transactions, where each line contains a list of grocery items purchased in a single transaction.


 
## Objectives:
- **Data Analysis**: Perform basic tasks such as counting transactions, finding the most and least frequently purchased items.
- **Market Basket Analysis**: Implement techniques like Support and Confidence to analyze relationships between items and discover customer purchasing patterns.


 
## Dataset:
| **Filename**       | **Description**                                               |
|--------------------|---------------------------------------------------------------|
| `grocery_data.csv` | Contains grocery transaction data, each line representing a single transaction with a list of grocery items. |


 
## Tasks:
### (a) Data Processing
- Read `grocery_data.csv` and store the content using Spark RDDs.
- Strip off trailing spaces and convert item names to lowercase.
- Find the total number of transactions.
- Identify the basket with the most items and display the content.

![Output of Task a](assets/Output%20of%20Task%20a.png)


### (b) Frequency Analysis
- Find the top 20 most frequently purchased items, including their occurrences and corresponding percentages.
- Find the bottom 20 least frequently purchased items, including their occurrences and corresponding percentages.

![Output of Task b](assets/Output%20of%20Task%20b.png)


### (c) Market Basket Analysis (Item Pairing)
- Assign an index to each transaction (0, 1, …).
- Find all possible 2-item combinations in the transactions and display them.
- Identify transaction indices where each item pair appears and compute the frequency of each item pair.

![Output of Task c I](assets/Output%20of%20Task%20c%201.png)
![Output of Task c II](assets/Output%20of%20Task%20c%202.png)


### (d) Support Metric Calculation
- Compute the support for each item pair based on their frequency and total number of transactions.
- Sort the item pairs by their occurrence count from highest to lowest.
- Display the top and bottom 20 item pairs sorted by their occurrence counts.

![Output of Task d](assets/Output%20of%20Task%20d.png)
![Output of Task d Top 20 Bottom 20](assets/Output%20of%20Task%20d%20Top%2020%20Bottom%2020.png)


### (e) Confidence Metric Calculation
- Calculate confidence for item pairs based on their frequency in transactions.
- Find the total occurrences of item pairs, sorted from highest to lowest.
- Compute the confidence for each item pair and display the results.

![Output of Task e](assets/Output%20of%20Task%20e.png)
![Output of Task e Compute Frequency X](assets/Output%20of%20Task%20e%20Compute%20Frequency%20X.png)


### (f) Confidence Analysis
- Compute and display the Confidence metric, which indicates the likelihood of purchasing item Y when item X is purchased.
- Display the top and bottom 20 item pairs sorted by their occurrence counts.

![Output of Task f](assets/Output%20of%20Task%20f.png)

 
## Requirements:

### Data Sources:
| **Filename**       | **Description**                                               |
|--------------------|---------------------------------------------------------------|
| `grocery_data.csv` | Contains grocery transaction data, each line representing a single transaction with a list of grocery items. |

### Software:
| **Software**        | **Version**  | **Description**                                                   |
|---------------------|--------------|-------------------------------------------------------------------|
| **Apache Spark**     | 3.x or later | Required for running PySpark jobs using RDDs.                     |
| **Python**           | 3.6 or later | Required for running PySpark scripts.                             |
| **Java**             | 8 or later   | Java is required by Spark for execution.                          |

### Python Libraries:
| **Library**         | **Version**  | **Description**                                                   |
|---------------------|--------------|-------------------------------------------------------------------|
| **PySpark**         | 3.x or later | For performing distributed data processing using RDDs.            |
| **Pandas**          | 1.x or later | For optional data manipulation and analysis in Python.            |
| **NumPy**           | 1.x or later | For numerical computations, if necessary.                         |
| **Matplotlib**      | Optional     | For visualizing the results, if desired.                          |


 
### Installation Instructions:
1. **Install Apache Spark**: Follow the instructions from the [official Spark website](https://spark.apache.org/downloads.html) to download and install Spark.
2. **Install Java**: Download and install [Java 8](https://adoptopenjdk.net/) (or later) if it's not already installed.
3. **Set up the Environment**:
   - Set the `SPARK_HOME` environment variable to your Spark installation directory.
   - Add the `bin/` directory of your Spark installation to your `PATH` environment variable.
4. **Install Python Libraries**:
   Install the required Python libraries using `pip`:
   ```bash
   pip install pyspark pandas numpy matplotlib
   ```

### Run the Script
1. Navigate to the directory containing the script:
   ```bash
   cd path/to/script
   ```
2. Execute the script with Python:
   ```bash
   python 'Grocery Market Basket Analysis using PySpark RDD.py'
   ```
