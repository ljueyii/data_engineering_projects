# British Monarch Family Tree Scraping and Database Integration

This project demonstrates a hands-on approach to web scraping, relational database design, and querying data to explore relationships within the British monarchy. Using Python libraries like BeautifulSoup and SQLite3, this project scrapes data, stores it in an SQLite database, and explores various operations related to family tree structures.

 
 
## Objectives:
- **Manipulate the dataset** with a data scientist mindset.
- **Scrape and parse information** from a real-world dataset (Wikipedia).
- **Store and manage data** in an SQLite database.
- **Perform ETL, and calculation operations** using Pandas and SQLite3.
- **Design and use relational database methods** to handle parent-child relationships and perform queries.


 
## Tasks:
### a) Scrape the Family Tree of British Monarchs
1. Scrape the family tree of British monarchs from the Wikipedia page using the **BeautifulSoup** library.
2. Store the scraped monarchs' data in an SQLite table `british_monarch_family_tree` with the following fields:
- `id`: Primary key
- `name`: Monarch's name
- `wiki_url`: Monarch’s Wikipedia URL

### b) Add Parent Information to the Family Tree
1. Modify the `british_monarch_family_tree` table to include two new fields:
- `father_id`: Foreign key referencing the `id` of the monarch’s father
- `mother_id`: Foreign key referencing the `id` of the monarch’s mother
2. Scrape the father and mother information from the respective monarch's Wikipedia pages.
3. If no information is available, set `null` values for missing parents.
4. If a parent does not exist in the table, add them and set the relationship (`father_id` and `mother_id`).

### c) Query Children of King "George III"
- Use SQLite3 to find and return all children of King George III using his Wikipedia page [here](https://en.wikipedia.org/wiki/George_III).

### d) Query Father and Mother of King "George III"
- Use SQLite3 to find the father and mother of King George III from the database.

### e) Query Siblings of King "George IV"
- Use SQLite3 to find all siblings of King George IV using his Wikipedia page [here](https://en.wikipedia.org/wiki/George_IV).

### f) Query Descendants of "Queen Victoria"
- Use SQLite3 and Pandas DataFrame to find all descendants of [Queen Victoria](https://en.wikipedia.org/wiki/Queen_Victoria) from the database.


 
## DataFrame Schema:
| **Column Name** | **Data Type** | **Description**                                                                                             |
|-----------------|---------------|-------------------------------------------------------------------------------------------------------------|
| `id`            | Integer       | The unique identifier for each monarch.                                                                     |
| `name`          | String        | The name of the monarch.                                                                                    |
| `wiki_url`      | String        | The relative URL path to the monarch's Wikipedia page <br> (e.g., `/wiki/George_IV_of_the_United_Kingdom`). |
| `father_id`     | Float/Null    | References the ID of the monarch's mother (nullable).                                                       |
| `mother_id`     | Float/Null    | References the ID of the monarch's mother (nullable).                                                       |


 
## Project Structure:
```
/root
├── /src
│   ├── British Monarch Family Tree Scraping and Database Integration.ipynb                     

```


 
## Requirements:
```
pip install pandas beautifulsoup4 sqlite3 requests
```