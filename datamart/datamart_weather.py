print("=== Script datamart_weather.py démarré ===")
import os
from logger import get_logger
from pyspark.sql.functions import col, count, avg
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession

def run_datamart_weather(spark, mysql_url, mysql_properties):
    print("=== Début run_datamart_weather ===")
    logger = get_logger("datamart_weather")
    df = spark.sql("SELECT * FROM silver.accidents")
    print("=== Colonnes du DataFrame accidents ===")
    print(df.columns)
    df.printSchema()
    df = df.withColumn("Severity", col("Severity").cast(IntegerType()))
    dm_weather = df.groupBy("State") \
        .agg(
            count("ID").alias("accident_count"),
            avg("Severity").alias("avg_severity")
        )
    gold_path_weather = os.path.join("Gold", "accidents_by_weather")
    dm_weather.write.mode("overwrite").parquet(gold_path_weather)
    logger.info(f"Datamart Parquet écrit dans {gold_path_weather}")
    logger.info(f"dm_weather schema: {dm_weather.schema}")
    logger.info(f"dm_weather row count: {dm_weather.count()}")
    logger.info(f"dm_weather sample data:")
    dm_weather.show(10, truncate=False)
    dm_weather = dm_weather.withColumnRenamed("State", "Weather_Condition")
    dm_weather = dm_weather.select("Weather_Condition", "accident_count", "avg_severity")
    try:
        dm_weather.write.mode("append").jdbc(
            url=mysql_url,
            table="gold_accidents_by_weather",
            properties=mysql_properties
        )
        logger.info("Datamart exporté dans MySQL (table gold_accidents_by_weather)")
    except Exception as e:
        logger.error(f"Erreur export MySQL (weather) : {e}")

if __name__ == "__main__":
    print("=== SparkSession en cours de création ===")
    spark = SparkSession.builder \
        .appName("DatamartWeather") \
        .enableHiveSupport() \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    print("=== SparkSession créée ===")

    mysql_url = "jdbc:mysql://localhost:3309/accidents?serverTimezone=UTC"
    mysql_properties = {
        "user": "username",
        "password": "password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    run_datamart_weather(spark, mysql_url, mysql_properties)
    spark.stop()
