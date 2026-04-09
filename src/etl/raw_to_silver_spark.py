import os
from pyspark.sql import SparkSession
from pyspark.sql import function as F
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    FloatType, BooleanType, ArrayType, DoubleType
)
from dotenv import load_dotenv

load_dotenv()

BUCKET_RAW = os.getenv("S3_BUCKET_RAW", "ml-pipeline-raw")
BUCKET_SILVER = os.getenv("S3_BUCKET_SILVER", "ml-pipeline-silver")
ENDPOINT_URL = os.getenv("AWS_ENDPOINT")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")


# We define the schema of our data: Spark will enforce it

FLIGHTS_SCHEMA = StructType([
    StructField("icao24", StringType(), True),
    StructField("firstSeen", LongType(), True),
    StructField("estDepartureAirport", StringType(), True),
    StructField("lastSeen", LongType(), True),
    StructField("estArrivalAirport", StringType(), True),
    StructField("callsign", StringType(), True),
    StructField("estDepartureAirportHorizDistance", LongType(), True),
    StructField("estDepartureAirportVertDistance", LongType(), True),
    StructField("estArrivalAirportHorizDistance", LongType(), True),
    StructField("estArrivalAirportVertDistance", LongType(), True),
    StructField("departureAirportCandidatesCount", LongType(), True),
    StructField("arrivalAirportCandidatesCount", LongType(), True),
])

WEATHER_HOURLY_SCHEMA = StructType([
    StructField("time", ArrayType(StringType()), True),
    StructField("temperature_2m", ArrayType(FloatType()), True),
    StructField("relative_humidity_2m", ArrayType(FloatType()), True),
    StructField("wind_speed_10m", ArrayType(FloatType()), True),
    StructField("precipitation", ArrayType(FloatType()), True),
    StructField("weather_code", ArrayType(FloatType()), True),
])

def build_spark_session() -> SparkSession:
    """
    Spark session creation
    ======================
    This function creates the session allowing Spark to interact with S3 buckets through Hadoop
    """

    builder = (
        SparkSession.builder
        .appName("raw_to_silver")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    )
    spark = builder.getOrCreate()

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_KEY)
    if ENDPOINT_URL:
        hadoop_conf.set("fs.s3a.endpoint", ENDPOINT_URL.replace("http://", "").replace("https://", ""))
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    return spark

def process_flights(spark: SparkSession, date: str) -> None:
    """
    This function fetches raw flights from the bronze S3 bucket and push them into the silver S3 bucket after
    performing feature operations
    """
    year, month, day = date.split("-")
    path = f"s3a://{BUCKET_RAW}/flights/year={year}/month={month}/day={day}/"

    df = spark.read.schema(FLIGHTS_SCHEMA).json(path)

    df = (
        df
        .dropna(subset=["icao24", "firstSeen", "lastSeen", "estDepartureAirport"])
        .dropDuplicates(["icao24", "firstSeen"])
        .withColumn("firstSeen_ts", F.to_timestamp(F.from_unixtime("firstSeen")))
        .withColumn("lastSeen_ts", F.to_timestamp(F.from_unixtime("lastSeen")))
        .withColumn("duration_minutes",
                    (F.col("lastSeen") - F.col("firstSeen")) / 60)
        .withColumn("year", F.lit(int(year)))
        .withColumn("month", F.lit(int(month)))
    )

    out_path = f"s3a:://{BUCKET_SILVER}/flights/"
    df.write.mode("overwrite").partitionBy("year", "month").parquet(out_path)
    print(f"Flights written to {out_path} - {df.count()} rows")

def process_weather(spark: SparkSession, airport: str, date: str) -> None:
    year, month, day = date.split("-")
    path = f"s3a://{BUCKET_RAW}/weather/year={year}/month={month}/day={day}/{airport}.json"

    raw = spark.read.json(path)

    hourly = raw.select(F.col("hourly.*"))

    arrays_col = [c for c in hourly.columns]
    max_len = hourly.select(F.size(F.col(arrays_col[0])).alias("n")).first()["n"]

    df = hourly.select(
        F.posexplode(F.col("time")).alias("pos", "time"),
        *[F.col(c) for c in arrays_col if c != "time"]
    ).select(
        "pos", "time",
        *[F.col(c)[F.col("pos")].alias(c) for c in arrays_col if c != "time"]
    ).drop("pos")

    df = (
        df
        .dropna(subset=["time", "temperature_2m"])
        .withColumn("timestamp", F.to_timestamp("time"))
        .withColumn("airport", F.lit(airport))
        .withColumn("year", F.lit(int(year)))
        .withColumn("month", F.lit(int(month)))
    )

    out_path = f"s3a://{BUCKET_SILVER}/weather/"
    df.write.mode("overwrite").partitionBy("year", "month").parquet(out_path)
    print(f"Weather for {airport} written to {out_path} — {df.count()} rows")



if __name__ == "__main__":
    DATE = "2024-01-15"
    AIRPORTS = ["EDDF", "EGLL"]

    spark = build_spark_session()
    process_flights(spark, DATE)
    for airport in AIRPORTS:
        process_weather(spark, airport, DATE)
    spark.stop()

