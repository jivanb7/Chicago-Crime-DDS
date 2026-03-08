import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_spark(app_name: str) -> SparkSession:
    uri = os.environ.get("MONGO_URI", "...")
    if not uri:
        raise RuntimeError("MONGO_URI env var is empty")

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.mongodb.read.connection.uri", uri)
        .config("spark.mongodb.write.connection.uri", uri)
        .getOrCreate()
    )


def normalize_for_model(raw_df: DataFrame) -> DataFrame:
    df = raw_df

    # Drop Mongo _id early
    if "_id" in df.columns:
        df = df.drop("_id")

    # Ensure event_ts exists
    if "event_ts" not in df.columns:
        if "date" in df.columns:
            df = df.withColumn("event_ts", F.to_timestamp("date"))
        elif "Date" in df.columns:
            df = df.withColumn("event_ts", F.to_timestamp(F.col("Date")))
        else:
            df = df.withColumn("event_ts", F.lit(None).cast("timestamp"))

    # Core columns (case-insensitive-ish fallbacks)
    if "district" not in df.columns and "District" in df.columns:
        df = df.withColumnRenamed("District", "district")

    if "primary_type" not in df.columns and "Primary Type" in df.columns:
        df = df.withColumnRenamed("Primary Type", "primary_type")

    if "arrest" not in df.columns and "Arrest" in df.columns:
        df = df.withColumnRenamed("Arrest", "arrest")

    # Types
    if "district" in df.columns:
        df = df.withColumn("district", F.col("district").cast("int"))

    if "primary_type" in df.columns:
        df = df.withColumn("primary_type", F.trim(F.col("primary_type")))

    if "arrest" in df.columns:
        df = df.withColumn(
            "arrest",
            F.when(F.col("arrest").cast("string").isNull(), F.lit(None))
            .otherwise(
                F.lower(F.col("arrest").cast("string")).isin("true", "1", "t", "yes", "y")
            ),
        )

    # Time features
    df = df.withColumn("hour", F.hour("event_ts"))
    df = df.withColumn("dow", F.dayofweek("event_ts"))  # 1=Sun ... 7=Sat
    df = df.withColumn("month", F.month("event_ts"))

    # Keep only what we need
    keep = ["event_ts", "district", "primary_type", "hour", "dow", "month", "arrest"]
    keep = [c for c in keep if c in df.columns]
    df = df.select(*keep)

    # Drop rows missing required keys
    df = df.filter(
        F.col("event_ts").isNotNull()
        & F.col("district").isNotNull()
        & F.col("primary_type").isNotNull()
        & F.col("hour").isNotNull()
        & F.col("dow").isNotNull()
        & F.col("month").isNotNull()
        & F.col("arrest").isNotNull()
    )

    return df


def agg_arrest_rate_by_context(df_norm: DataFrame, min_bucket_n: int) -> DataFrame:
    base = df_norm.withColumn("arrest_int", F.when(F.col("arrest") == True, 1).otherwise(0))

    agg = (
        base.groupBy("district", "primary_type", "hour", "dow", "month")
        .agg(
            F.count(F.lit(1)).alias("n_incidents"),
            F.avg(F.col("arrest_int")).alias("arrest_rate"),
        )
        .filter(F.col("n_incidents") >= F.lit(int(min_bucket_n)))
        .orderBy(F.desc("n_incidents"))
    )

    # Optional binary label for a pure classification dataset at the bucket level
    # You can tune this threshold later.
    agg = agg.withColumn("label_arrest_high", F.when(F.col("arrest_rate") >= F.lit(0.5), 1).otherwise(0))

    return agg


def build_training_table(df_norm: DataFrame, agg_df: DataFrame) -> DataFrame:
    joined = (
        df_norm.join(
            agg_df,
            on=["district", "primary_type", "hour", "dow", "month"],
            how="left",
        )
        .withColumn("label", F.when(F.col("arrest") == True, 1).otherwise(0))
        .withColumn("log_n_incidents", F.log1p(F.col("n_incidents")))
        .select(
            "label",
            "district",
            "primary_type",
            "hour",
            "dow",
            "month",
            "arrest_rate",
            "n_incidents",
            "log_n_incidents",
        )
    )

    # Drop rows where the bucket was filtered out (no agg match)
    joined = joined.filter(F.col("arrest_rate").isNotNull())

    return joined


def write_mongo(df: DataFrame, db: str, coll: str, mode: str) -> None:
    print("Writing to Mongo:", f"{db}.{coll}")
    (
        df.write.format("mongodb")
        .mode(mode)
        .option("database", db)
        .option("collection", coll)
        .save()
    )


def preview(name: str, df: DataFrame, n: int = 10) -> None:
    print("\n===", name, "===")
    df.show(n, truncate=False)


def main() -> None:
    mongo_db = os.environ.get("MONGO_DB", "crime")
    raw_coll = os.environ.get("MONGO_RAW_COLL", os.environ.get("MONGO_COLL", "chicago_crime"))

    # Derived collections
    agg_coll = os.environ.get("MONGO_AGG_COLL", "chicago_crime_agg_context")
    train_coll = os.environ.get("MONGO_TRAIN_COLL", "chicago_crime_train")

    # Controls
    write_mode = os.environ.get("WRITE_MODE", "overwrite")  # overwrite for derived tables is easiest
    test_limit = int(os.environ.get("TEST_LIMIT_ROWS", "0"))
    min_bucket_n = int(os.environ.get("MIN_BUCKET_N", "20"))

    print("=== START mongo clean + agg job ===")
    print("MONGO_DB:", mongo_db)
    print("RAW_COLL:", raw_coll)
    print("AGG_COLL:", agg_coll)
    print("TRAIN_COLL:", train_coll)
    print("WRITE_MODE:", write_mode)
    print("TEST_LIMIT_ROWS:", test_limit)
    print("MIN_BUCKET_N:", min_bucket_n)

    spark = build_spark("crime_mongo_clean_agg")

    print("Reading raw from Mongo...")
    raw_df = (
        spark.read.format("mongodb")
        .option("database", mongo_db)
        .option("collection", raw_coll)
        .load()
    )

    if test_limit > 0:
        raw_df = raw_df.limit(test_limit)

    raw_count = raw_df.count()
    print("Raw rows:", raw_count)

    print("Normalizing...")
    df_norm = normalize_for_model(raw_df)
    norm_count = df_norm.count()
    print("Normalized rows:", norm_count)

    preview("normalized_sample", df_norm, n=5)

    print("Aggregating arrest rate by context...")
    agg_df = agg_arrest_rate_by_context(df_norm, min_bucket_n=min_bucket_n)
    preview("agg_arrest_rate_by_context", agg_df, n=10)

    print("Building training table...")
    train_df = build_training_table(df_norm, agg_df)
    preview("training_table_sample", train_df, n=10)

    print("Writing derived collections...")
    write_mongo(agg_df, db=mongo_db, coll=agg_coll, mode=write_mode)
    write_mongo(train_df, db=mongo_db, coll=train_coll, mode=write_mode)

    print("=== DONE ===")
    spark.stop()


if __name__ == "__main__":
    main()