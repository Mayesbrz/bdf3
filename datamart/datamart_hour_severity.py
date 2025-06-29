import os
from logger import get_logger
from pyspark.sql.functions import col, count, avg, hour, concat, lpad, lit
from pyspark.sql.types import IntegerType

def run_datamart_hour_severity(spark, mysql_url, mysql_properties):
    logger = get_logger("datamart_hour_severity")
    df = spark.sql("SELECT * FROM silver.accidents")
    df = df.withColumn("Severity", col("Severity").cast(IntegerType()))
    df = df.withColumn("hour", hour(col("Start_Time")))
    df = df.withColumn("hour_str", concat(lpad(col("hour").cast("string"), 2, "0"), lit(":00")))
    dm_hour = df.groupBy("hour_str", "Severity") \
        .agg(
            count("ID").alias("accident_count"),
            avg("Severity").alias("avg_severity")
        )
    gold_path_hour = os.path.join("Gold", "accidents_by_hour_severity")
    dm_hour.write.mode("overwrite").parquet(gold_path_hour)
    logger.info(f"Datamart Parquet écrit dans {gold_path_hour}")
    dm_hour = dm_hour.select("hour_str", "Severity", "accident_count", "avg_severity")
    dm_hour = dm_hour.withColumnRenamed("hour_str", "hour")
    try:
        dm_hour.write.mode("append").jdbc(
            url=mysql_url,
            table="gold_accidents_by_hour_severity",
            properties=mysql_properties
        )
        logger.info("Datamart exporté dans MySQL (table gold_accidents_by_hour_severity)")
    except Exception as e:
        logger.error(f"Erreur export MySQL (hour/severity) : {e}")
