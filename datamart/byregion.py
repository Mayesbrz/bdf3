import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger import get_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, to_date
import os

def run_datamart():
    logger = get_logger("datamart")
    logger.info("Démarrage du datamart.")
    spark = SparkSession.builder \
        .appName("DatamartGold") \
        .enableHiveSupport() \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    df = spark.sql("SELECT * FROM silver.accidents")
    df = df.withColumn("date", to_date(col("Start_Time")))
    dm = df.groupBy("State", "County", "City", "Severity", "date") \
        .agg(
            count("ID").alias("accident_count"),
            avg("Severity").alias("avg_severity")
        )
    gold_path = os.path.join("Gold", "accidents_by_region")
    dm.write.mode("overwrite").parquet(gold_path)
    logger.info(f"Datamart Parquet écrit dans {gold_path}")

    mysql_url = "jdbc:mysql://localhost:3309/accidents?serverTimezone=UTC"
    mysql_properties = {
        "user": "username",  
        "password": "password",  
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    dm = dm.select("State", "County", "City", "Severity", "date", "accident_count", "avg_severity")
    try:
        dm.write.mode("append").jdbc(
            url=mysql_url,
            table="gold_accidents_by_region",
            properties=mysql_properties
        )
        logger.info("Datamart exporté dans MySQL (table gold_accidents_by_region)")
    except Exception as e:
        logger.error(f"Erreur export MySQL : {e}")
    spark.stop()
    logger.info("Fin du datamart.")

if __name__ == "__main__":
    run_datamart()