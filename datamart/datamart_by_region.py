from logger import get_logger
from pyspark.sql.functions import col, count, avg, to_date
import os

def run_datamart_by_region(spark, mysql_url, mysql_properties):
    logger = get_logger("datamart_by_region")
    logger.info("Démarrage du datamart par région.")
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
    dm = dm.select("State", "County", "City", "Severity", "date", "accident_count", "avg_severity")
    try:
        dm.write.mode("append").jdbc(
            url=mysql_url,
            table="gold_accidents_by_region",
            properties=mysql_properties
        )
        logger.info("Datamart exporté dans MySQL (table gold_accidents_by_region)")
    except Exception as e:
        logger.error(f"Erreur export MySQL (by_region) : {e}")
    logger.info("Fin du datamart par région.")
