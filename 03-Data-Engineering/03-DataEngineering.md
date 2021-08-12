# Introduction

## Cloud Providers
 Storage, Computation, and Databases

### Storage (unstructured)
- AWS S3
- Azure blob storage
- GCP cloud storage

### Computation
- AWS EC2
- Azure VMs
- GCP Compute Engine

### Databases
- AWS RDS (SQL)
- (Azure) SQL Database
- (GCP) Cloud SQL


# Data Engineeering Toolbox
## Databaases
(classification according to the course):
- Structured (relational)
- Semi-structured (e.g. JSON)
- unstructured (schemaless), e.g. blobs (videos)

### NoSQL
- structured or unstructured
- key-value store (Redis)
- document-based (mongodb)

You can use pandas to query SQL database

Need a `db_engine`

```python
import pandas as pd

data = pd.read_sql("""
SELECT first_name, last_name FROM "Customer"
ORDER BY last_name, first_name
""", db_engine)

# Show the first 3 rows of DataFrame:
print(data.head(3))

# Show info:
print(data.info())
```

## Parallel processing
At a low level, we could use the `multiprocessing.Pool` API to distribute work over several cores on the same machine. 

```python
from multiprocessing import Pool

def take_mean_age(year_and_group):
    year, group = year_and_group
    return pd.DataFrame({"Age": group["Age"].mean()}, index=[year])

# 4 stands for 4 cores
with Pool(4) as p:
    results = p.map(take_mean_age, athlete_events.groupby("Year"))

# "reduce":
results_df = pd.concat(results)
```

`dask` package has a parallel-enabled dataframe

```python
import dask.dataframe as dd

# partition dataframe into 4
athlete_events_dask = dd.from_pandas(athlete_events, npartitions=4)

# run parallel computations on each partition
# `compute()` actually does the work <= lazy evaluation
result_df = athlete_events_dask.groupby('Year').Age.mean().compute()

```
## Parallel computing frameworks
### Hadoop (foundations? old school)
- MapReduce
- HDFS

Nowadays: S3 replaces HDFS

Hard to write MapReduce jobs => **Hive** came up, uses Hive SQL
- looks just like SQL

Does "too many" disk writes

Hive integrates with other data processing toolds

### Spark
As much processing as possible in memory

Resilient distributed datasets (RDDs)
- data structure that maintains data distributed
- don't have named columns
- list of tuples
- transformations: .map() or .filter()
- or actions: .count(), first()
- PySpark: interface to Spark
- PySpark has DataFrame abstraction

#### Pyspark example
```python
# Load the dataset into athlete_events_spark first
(athlete_events_spark
    .groupBy('Year')
    .mean('Age')
    .show())
```

```python 
# Print the type of athlete_events_spark
print(type(athlete_events_spark))

# Print the schema of athlete_events_spark
print(athlete_events_spark.printSchema())

# Group by the Year, and find the mean Age
print(athlete_events_spark.groupBy('Year').mean('Age'))

# Group by the Year, and find the mean Age
print(athlete_events_spark.groupBy('Year').mean('Age').show())
```

#### Spark file to run a job:


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    athlete_events_spark = (spark
        .read
        .csv("/home/repl/datasets/athlete_events.csv",
             header=True,
             inferSchema=True,
             escape='"'))

    athlete_events_spark = (athlete_events_spark
        .withColumn("Height",
                    athlete_events_spark.Height.cast("integer")))

    print(athlete_events_spark
        .groupBy('Year')
        .mean('Height')
        .orderBy('Year')
        .show())


#### to submit the file:

spark-submit --master local[4] <path to spark script.py>


## Workflow scheduling frameworks
- cron
- Spotify's Luigi
- Apache Airflow (airbnb)

Example of Apache Airflow:
```python
# create DAG:
dag = DAG(dag_id = "example_dag", ..., schedule_interval="0 * * * *")

# define operations
start_cluster = StartClusterOperator(task_id="start_cluster", dag=dag)
ingest_customer_data = SparkJobOperator(task_id="ingest_customer_data", dag=dag)
ingest_product_data = SparkJobsOperator(task_id = "ingest_product_data", dag=dag)
enrich_customer_data = PythonOperator(task_id="enrich_customer_data", ..., dag=dag)

# Set up dependency flow - connections between operators
start_cluster.set_downstream(ingest_customer_data)
ingest_customer_data.set_downstream(enrich_customer_data)
ingest_product_data.set_downstream(enrich_customer_data)
```

### Airflow operators:
- BashOperator
- PythonOperator
- own operators
- SparkJobOperator
- StartClusterOperator (from example)


```python
# Exercise:
# Create the DAG object
# 0 * * * * means runs every hour on the hour (0th minute)
dag = DAG(dag_id="car_factory_simulation",
          default_args={"owner": "airflow","start_date": airflow.utils.dates.days_ago(2)},
          schedule_interval="0 * * * *")

# Task definitions
assemble_frame = BashOperator(task_id="assemble_frame", bash_command='echo "Assembling frame"', dag=dag)
place_tires = BashOperator(task_id="place_tires", bash_command='echo "Placing tires"', dag=dag)
assemble_body = BashOperator(task_id="assemble_body", bash_command='echo "Assembling body"', dag=dag)
apply_paint = BashOperator(task_id="apply_paint", bash_command='echo "Applying paint"', dag=dag)

# Complete the downstream flow
assemble_frame.set_downstream(place_tires)
assemble_frame.set_downstream(assemble_body)
assemble_body.set_downstream(apply_paint)
```

# ETL
## Extract
### Reading JSON files
JSON objects are mapped to Python dictionaries
```python
import json
result = json.loads('{"key_1": "value_1",
        "key_2": "value_2"}')
print(result["key_1"])
```

### Calling an API
```python
import requests
response = requests.get("https://...")
print(response.json())
```

### Querying databases
```python
import sqlalchemy
connection_uri = 'postgresql://repl:password@localhost:5432/pagila'
db_engine = sqlalchemy.create_engine(connection_uri)

# Then can use it from pandas:
import pandas  as pd
pd.read_sql("SELECT last_name from customer", db_engine)
```

Exercise code: API
```python
import requests

# Fetch the Hackernews post
resp = requests.get("https://hacker-news.firebaseio.com/v0/item/16222426.json")

# Print the response parsed as JSON
print(resp.json())

# Assign the score of the test to post_score
post_score = resp.json()["score"]
print(post_score)
```

Exercise code: reading from database:
```python
# Function to extract table to a pandas DataFrame
def extract_table_to_pandas(tablename, db_engine):
    query = "SELECT * FROM {}".format(tablename)
    return pd.read_sql(query, db_engine)

# Connect to the database using the connection URI
connection_uri = "postgresql://repl:password@localhost:5432/pagila" 
db_engine = sqlalchemy.create_engine(connection_uri)

# Extract the film table into a pandas DataFrame
extract_table_to_pandas("film", db_engine)

# Extract the customer table into a pandas DataFrame
extract_table_to_pandas("customer", db_engine)
```

## Transform
e.g. Splitting the email address with pandas

## Load
## Summary