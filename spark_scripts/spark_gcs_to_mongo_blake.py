from __future__ import annotations
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


GCS_PATH = "gs://blakes_chicago_bucket/crime-*"
# GCS_PATH = os.environ.get("GCS_PATH", "gs://airdata-bucket/dataset/crime-*.csv")
MONGO_URI = "mongodb://localhost:27017/"
# MONGO_URI = os.environ.get("MONGO_URI", "mongodb+srv://dbuser:2IpbYCFEWj5nij77@cluster0.xsanuz.mongodb.net/?appName=Cluster0")
MONGO_DB = "crime"
# MONGO_DB = os.environ.get("MONGO_DB", "crime")
MONGO_COLLECTION = "chicago_crime"
# MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "chicago_crime")

def main() -> None:
    # spark = (
    #     SparkSession.builder.appName("crime_gcs_to_mongo")
    #     .config("spark.mongodb.read.connection.uri", MONGO_URI)
    #     .config("spark.mongodb.write.connection.uri", MONGO_URI)
    #     .getOrCreate()
    # )
    adc = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")

    spark = (
        SparkSession.builder.appName("crime_gcs_to_mongo")
        .config(
            "spark.jars.packages",
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.20,"
            "org.mongodb.spark:mongo-spark-connector_2.13:11.0.0"
        )
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")

        # GCS authentication
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", adc)

        # Mongo connection
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.connection.uri", MONGO_URI)

        .getOrCreate()
    )


    df = spark.read.csv(GCS_PATH, header=True, mode="PERMISSIVE")
    print("✅ read ok")
    print("rows:", df.count())
    df = df.select([F.when(F.col(c) == "", None).otherwise(F.col(c)).alias(c) for c in df.columns])

    rename_map = {
        "Unique Key": "unique_key",
        "Case Number": "case_number",
        "Primary Type": "primary_type",
        "Date": "date",
        "Updated On": "updated_on",
        "Arrest": "arrest",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.withColumnRenamed(old, new)

    df = df.filter(F.col("unique_key").isNotNull())
    df = df.withColumn("unique_key", F.col("unique_key").cast("long"))
    df = df.withColumn("_id", F.col("unique_key"))

    for c in ["year", "beat", "district", "ward", "community_area", "x_coordinate", "y_coordinate"]:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("int"))
    for c in ["latitude", "longitude"]:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    strip_utc = lambda col: F.to_timestamp(F.regexp_replace(F.col(col), r"\sUTC$", ""), "yyyy-MM-dd HH:mm:ss")
    df = df.withColumn("date", strip_utc("date"))
    df = df.withColumn("updated_on", strip_utc("updated_on"))
    df = df.withColumn("arrest", F.lower(F.col("arrest")).isin("true", "1", "t", "yes", "y"))

    df = df.dropDuplicates(["_id"])

    (
        df.write.format("mongodb")
        .mode("append")
        .option("database", MONGO_DB)
        .option("collection", MONGO_COLLECTION)
        .save()
    )
    
    print("✅ write finished")
    spark.stop()

if __name__ == "__main__":
    main()
