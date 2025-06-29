import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger import get_logger
from pyspark.sql import SparkSession
from datamart_hour_severity import run_datamart_hour_severity
from datamart_weather import run_datamart_weather
from datamart_by_region import run_datamart_by_region

def run_datamarts():
    logger = get_logger("datamarts")
    logger.info("Démarrage des datamarts.")
    spark = SparkSession.builder \
        .appName("DatamartsGold") \
        .enableHiveSupport() \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    mysql_url = "jdbc:mysql://localhost:3309/accidents?serverTimezone=UTC"
    mysql_properties = {
        "user": "username",
        "password": "password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    results = {}
    for name, func in [
        ("hour_severity", run_datamart_hour_severity),
        ("weather", run_datamart_weather),
        ("by_region", run_datamart_by_region)
    ]:
        logger.info(f"--- Début datamart {name} ---")
        try:
            func(spark, mysql_url, mysql_properties)
            results[name] = "OK"
            logger.info(f"--- Fin datamart {name} (succès) ---")
        except Exception as e:
            results[name] = f"ERREUR: {e}"
            logger.error(f"--- Fin datamart {name} (échec): {e} ---")

    spark.stop()
    logger.info("Fin des datamarts.")
    logger.info(f"Résumé exécution datamarts: {results}")

if __name__ == "__main__":
    run_datamarts()
