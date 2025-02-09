# COVID-19 TraceID Cluster Analysis (Part 2: Acquaintance Network Analysis)

This project is the second part of my COVID-19 TraceID Cluster Analysis, focusing on how the network of acquaintances between individuals can influence the development and spread of virus clusters. Using SQLAlchemy ORM, this analysis combines network data (`acquaintance.csv`) with the previous cluster data (`people.csv`) to gain insights into the interactions that lead to the formation of clusters.
 
## Objectives:
- **Understand the dataset** with a data scientist mindset, considering both the individual characteristics (`people.csv`) and network interactions (`acquaintance.csv`).

- **Design computation logic and routines** in Python using SQLAlchemy ORM to model relationships.

- **Conduct data visualisation** to examine network structure and its influence on cluster development.

- **Perform simple exploratory data analysis** on the acquaintance network to identify potential connections affecting the cluster.

- **Assess the design and use of a database ORM**, implementing ETL operations to analyse the acquaintance network data and its impact on virus spread.

 
## Dataset Files:

| **Filename**       | **Description**                                                                                                                         |
|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `people.csv`       | Contains person IDs and names of people in the population.                                                                              |
| `acquaintance.csv` | Contains pairings of individuals who know each other, with fields `from` and `to`, <br> representing two person IDs who are acquainted. |

 
## Tasks:

### (a) Define SQLAlchemy ORM Entities
Use SQLAlchemy ORM to define and store two entities:
- Person: Represents each individual in the network.
- Acquaintance: Represents the direct relationships between two individuals (Person to Person).

The relationship between the two entities is defined, where a Person can have multiple acquaintances. This will help understand how social connections can influence virus spread in the context of the previous cluster analysis.


### (b) Find Direct Acquaintances
Compose the necessary queries and define a function using SQLAlchemy ORM that:
- Takes a **person ID** as a parameter.
- Returns all **direct acquaintances** of that person (i.e., people who have a direct relationship with the given person ID).

**Direct relationship**: A direct edge between two people (e.g., `A <-> B`), representing potential transmission paths for the virus.

![Output of Task b](assets/Output%20of%20Task%20b.png)


### (c) Count and Visualise Acquaintances
(i) **Count the number of acquaintances per person** using SQLAlchemy ORM.

![Output of Task ci](assets/Output%20of%20Task%20ci.png)


(ii) **Draw a boxplot** to display the distribution of the number of acquaintances per person. This is crucial for understanding the network structure and the likelihood of individuals being exposed to the virus through social interactions.

![Output of Task cii](assets/Output%20of%20Task%20cii.png)


### (d) Find People with Most Acquaintances
Use SQLAlchemy ORM to find the **names of people** who have the most acquaintances. These individuals are more likely to be central nodes in the network and could play a key role in spreading the virus within clusters.

![Output of Task d](assets/Output%20of%20Task%20d.png)


### (e) Find Groups of Three People Who Know Each Other
Use SQLAlchemy ORM to find all groups of three distinct people who all know each other. This could help identify tightly-knit groups in the network that may experience more rapid virus transmission, further influencing cluster development.

 
## Project Structure:
```
/root
├── /assets
│   ├── Output of Task b.png
│   ├── Output of Task ci.png
│   ├── Output of Task cii.png
│   ├── Output of Task d.png
│   ├── Output of Task e.png
├── /data
│   ├── acquaintance.csv
│   └── people.csv
├── /src
│   ├── COVID-19 TraceID Cluster - Acquaintance Network Analysis.ipynb
├── requirements.txt
└── README.md
```

   
## Requirements:
You can install the necessary dependencies using pip:
```bash
pip install sqlalchemy pandas matplotlib
```

 