# Distributed Data Systems - Chicago Crime 

## Project Overview
This project explores distributed data processing using `Apache Spark` on `Google Cloud Dataproc`, 
with data stored in `Google Cloud Storage` and analytics performed using `MongoDB` and `SparkSQL`.

The project demonstrates:
- Distributed ingestion of large datasets from GCS.
- Aggregations using MongoDB aggregation pipelines.
- Analytical queries using SparkSQL.
- A classification model trained with Spark ML.
- End to end orchestration using Airflow.

## Environment Setup
Run this create env once.

```bash
conda env create -f environment.yml
conda activate crime
```

## Dataset - Chicago Crime
We use a **BigQuery public dataset**, exported to **Google Cloud Storage** as CSV shards.

- Dataset: [bigquery-public-data.chicago_crime.crime](https://console.cloud.google.com/bigquery?ws=!1m5!1m4!4m3!1sbigquery-public-data!2schicago_crime!3scrime)
- Local CSV Shard: `gs://airdata-bucket/dataset/chicago_crime_000000000000.csv`

The dataset contains reported crime incidents in Chicago with attributes such as date, location, crime type, arrest status, and district.  

---

## Project Structure
```
crime/
│
├── dags/
│   └── airflow_pipeline.py          # Orchestrates the full pipeline (run last)
│
├── mongo_notebooks/
│   ├── aadhya.ipynb                 # Mongo Aggregations
│   ├── blake.ipynb
│   ├── jivan.ipynb
│   └── sarah.ipynb
│
├── mongo_scripts/
│   ├── mongo_all_aggregations.py    # All Mongo Aggregations / Pipelines
│   └── run_mongo_aggregations.py    # Executes all Mongo aggregations
│
├── spark_scripts/
│   ├── spark_gcs_to_mongo.py        # Spark job: GCS → Mongo raw ingest
│   ├── spark_run_sparksql.py        # Spark job: SparkSQL analytics
│   └── spark_train_model.py         # Spark job: ML training
│
├── environment.yml                  # Conda Environment Packages
├── .gitignore                       
└── README.md
```

## Work Division
| Team Member | Notebook | Focus |
|------------|----------|-------|
| **Aadhya** | `aadhya.ipynb` | Mongo Aggregation |
| **Blake** | `blake.ipynb` | Mongo Aggregation |
| **Sarah** | `sarah.ipynb` | Mongo Aggregation |
| **Jivan Bal** | `jivan.ipynb` | Mongo Aggregation |

## Workflow Overview

**1. BigQuery → GCS Bucket**  
BigQuery exports the Chicago Crime dataset to a GCS bucket as CSV shards using a wildcard path.  
This step is handled online through the BigQuery UI.  

**2. GCS Bucket → Dataproc (Spark ingest)**  
Raw CSV shards are read from the GCS bucket into a Dataproc Spark cluster.  
Spark loads the data, applies basic schema handling, and prepares it for storage.  
- `spark_scripts/spark_gcs_to_mongo.py`

**3. Dataproc → MongoDB Atlas (raw data)**  
The same Spark job writes the raw crime records from Dataproc into MongoDB Atlas.  
This creates the raw MongoDB collection that all downstream steps rely on.
- `spark_scripts/spark_gcs_to_mongo.py`

**4. MongoDB Atlas (raw → derived aggregations)**   
MongoDB aggregation pipelines run directly inside Atlas to create derived collections.  
These aggregations are prototyped in notebooks and finalized in code.
- `mongo_scripts/mongo_all_aggregations.py`
- `mongo_scripts/run_mongo_aggregations.py`

**5. MongoDB Atlas → Dataproc (SparkSQL analytics)**  
Spark reads both raw and derived MongoDB collections back into Dataproc.  
SparkSQL queries are executed for analytical results, which are written to GCS.
- `spark_scripts/spark_run_sparksql.py`

**6. MongoDB Atlas → Dataproc (Spark ML training)**  
Spark reads MongoDB data again to build features and train a classification model using Spark ML.  
Model metrics and artifacts are written to GCS.
- `spark_scripts/spark_train_model.py`

**7. Airflow orchestration**  
Airflow ties all steps together and runs them in order.  
It triggers Spark jobs and Mongo aggregation execution and handles scheduling and retries.
- `dags/airflow_pipeline.py`

