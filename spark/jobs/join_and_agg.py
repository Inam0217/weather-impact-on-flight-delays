import os
from pyspark.sql import SparkSession, functions as F

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pass")
DB_NAME = os.getenv("DB_NAME", "airline_db")

def main():
    spark = (SparkSession.builder
             .appName("WeatherFlightJoin")
             .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
             .getOrCreate())

    url = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}?useSSL=false"

    flights = (spark.read.format("jdbc")
               .option("url", url)
               .option("dbtable", "flights")
               .option("user", DB_USER)
               .option("password", DB_PASSWORD)
               .load())

    weather = (spark.read.format("jdbc")
               .option("url", url)
               .option("dbtable", "weather_hourly")
               .option("user", DB_USER)
               .option("password", DB_PASSWORD)
               .load())

    # Join by origin airport and hour of scheduled departure
    flights_hour = flights.withColumn("ts_hour", F.date_trunc("hour", F.col("sched_dep_time")))
    joined = (flights_hour.alias("f")
              .join(weather.alias("w"),
                    (F.col("f.origin") == F.col("w.airport")) &
                    (F.col("f.ts_hour") == F.col("w.ts_hour")),
                    "left"))

    # Example aggregations
    agg = (joined.groupBy("f.origin", "w.weather_main")
                 .agg(F.avg("f.dep_delay_mins").alias("avg_dep_delay"),
                      F.count("*").alias("num_flights"))
                 .orderBy(F.desc("num_flights")))

    out_path = "data/processed/agg_delay_by_weather"
    agg.coalesce(1).write.mode("overwrite").option("header", True).csv(out_path)

    print(f"Wrote aggregations to {out_path}")
    spark.stop()

if __name__ == "__main__":
    main()
