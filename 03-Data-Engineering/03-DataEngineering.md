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

```python
customer_df # Pandas DataFrame with customer data

# split email column into 2 columns on the '@' character
split_email = customer_df.email.str.split('@', expand=True)

# create 2 new columns using split_email
customer_df = customer_df.assign(
    username=split_email[0]
    domain=split_email[1]
)
```

### Extract using PySpark:
```python
import pyspark.sql

spark = pyspark.sql.SparkSession.builder.getOrCreate()

# Read a table:
spark.read.jdbc('jdbc:postgresql://localhost:5432/pagila',
'customer', # table name
properties={'user':'repl', 'password':'password'})
```
### Transform using PySpark
Join Tables on the rating column
```python
# PySpark, not Pandas!
customer_df # PySpark DataFrame with customer data
ratings_df  #PySpark DataFrame with ratings data

# Group by ratings:
ratings_per_customer = ratings_df.groupBy('customer_id').mean('rating')

# Join on customer ID
customer_df.join(
    ratings_per_customer, 
    # "on" clause in the join: column to join on:
    customer_df.customer_id == ratings_per_customer.customer_id
)
```

## Load

"By storing data per column, it's faster to loop over these specific columns to resolve a query"

MPP
- Amazon RedShift
- Azure SQL Data Warehouse
- Google BigQuery

MPP load data better from columnar storage format, e.g. parquet, not CSV

```
# Pandas DF .to_parquet() method
df.to_parquet('./s3://path/to/bucket/customer.parquet')
# PySpark DF .write.parquet() method
df.write.parquet('./s3://path/to/bucket/customer.parquet')
```

```
COPY customer
FROM 's3://path/to/bucket/customer.parquet'
FORMAT as parquet
```

### load to PostgreSQL

```python
pandas.to_sql()
```

```python
# Transformation on data
recommendations = transform_find_recommendations(ratings_df)

# Load the transformed data into PostgreSQL:
recommendations.to_sql('recommendations',
            db_engine,
            schema='store',
            if_exists='replace')
```

```python
# Exercise: Load into Postgres:
connection_uri = "postgresql://repl:password@localhost:5432/dwh"
db_engine_dwh = sqlalchemy.create_engine(connection_uri)

# Transformation step, join with recommendations data
film_pdf_joined = film_pdf.join(recommendations)

film_pdf_joined.to_sql("film", 
db_engine_dwh,
schema="store",
if_exists="replace")

# Run the query to fetch the data
pd.read_sql("SELECT film_id, recommended_film_ids FROM store.film", db_engine_dwh)
```

## Summary

Clean ETF function!

e.g.
```python
# Extract function:
def extract_table_to_df(tablename, db_engine):
    return pd.read_sql('SELECT * FROM {}'.format(tablename), db_engine)

# Transforms:
def split_columns_transform(df, column, pat, suffixes):
    # something

def load_df_into_dwh(film_df, tablename, schema, db_engine):
    return pd.to_sql(tablename, db_engine, schema=schema, if_exists='replace')

db_engines = {} # Configured...

def etl():
    # Extract
    film_df = extract_table_to_df('film', db_engines['store'])
    # Transform
    film_df = split_columns_transform(film_df, 'rental_rate', '.', ['_dollar', '_cents'])
    # Load
    load_df_into_dwh(film_df, 'film', 'store', db_engines['dwh'])
```

Now, need to schedule (airflow):

```python
from airflow.models import DAG

from airflow.operators.python_operator import PythonOperator

dag = DAG(dag_id = 'etl_pipeline', ..., schedule_interval = '0 0 * * *')

etl_task = PythonOperator(task_id='etl_task', python_callable=etl, dag=dag)

# etl_task waits for "wait_for_this_task" to be finished
etl_task.set_upstream(wait_for_this_task)
```

Save DAG definition into a `*.py` file, place under `~/airflow/dags/`

Anatomy of a cron expression:
- minute
- hour
- day of the month
- month
- day of the week


## Setting up Airflow:
Move `dag.py` file to the folder defined in `airflow.cfg` under `dags_folder`


# DataCamp Case Study

## Querying the table
```python
# Complete the connection URI
connection_uri = "postgresql://repl:password@localhost:5432/datacamp_application"
db_engine = sqlalchemy.create_engine(connection_uri)

# Get user with id 4387
user1 = pd.read_sql("SELECT * FROM rating where user_id = 4387", db_engine)

# Get user with id 18163
user2 = pd.read_sql("SELECT * FROM rating where user_id = 18163", db_engine)

# Get user with id 8770
user3 = pd.read_sql("SELECT * FROM rating where user_id = 8770", db_engine)

# Use the helper function to compare the 3 users
print_user_comparison(user1, user2, user3)
```

# Average rating per course:
```python
# Complete the transformation function
def transform_avg_rating(rating_data):
    # Group by course_id and extract average rating per course
    avg_rating = rating_data.groupby('course_id').rating.mean()
    # Return sorted average ratings per course
    sort_rating = avg_rating.sort_values(ascending=False).reset_index()
    return sort_rating

# Extract the rating data into a DataFrame    
rating_data = extract_rating_data(db_engines)

# Use transform_avg_rating on the extracted data and print results
avg_rating_data = transform_avg_rating(rating_data)
print(avg_rating_data) 
```

