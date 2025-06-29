import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import get_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os
from datetime import datetime

def run_feeder():
    logger = get_logger("feeder")
    logger.info("Démarrage du feeder.")
    spark = SparkSession.builder \
        .appName("CSV to Parquet Feeder") \
        .getOrCreate()

    base_path = "Bronze"
    input_dir = "data"
    cumulative_df = None

    csv_files = [
        f for f in os.listdir(input_dir)
        if f.endswith(".csv") and len(f) >= 14
    ]
    csv_files.sort(key=lambda x: datetime.strptime(x.replace(".csv", ""), "%d-%m-%Y"))
    csv_files = [f for f in csv_files if f.endswith(".csv") and f[3:5] == "01" and f[6:10] == "2025"]
    csv_files = sorted(csv_files, key=lambda x: int(x[:2]))[:3]

    for i, filename in enumerate(csv_files):
        file_path = os.path.join(input_dir, filename)
        date_str = filename.replace(".csv", "")
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
        output_path = os.path.join(base_path, year, month, day)

        is_last_file = i == len(csv_files) - 1
        data_already_exists = os.path.exists(os.path.join(output_path, "data_complete" if is_last_file else ""))

        if data_already_exists:
            logger.info(f"Données déjà présentes pour le {date_str}, lecture pour accumulation uniquement.")
            df = spark.read.option("header", "true").option("escape", "\"").csv(file_path)
            for column in df.columns:
                if any(c in column for c in " ()%[].,;:{}"):
                    new_column = column \
                        .replace("(", "_").replace(")", "_") \
                        .replace("%", "pct").replace(".", "_") \
                        .replace(" ", "_")
                    df = df.withColumnRenamed(column, new_column)
            df = df.withColumn("situation_date", lit(dt.strftime("%Y-%m-%d")).cast("date"))
            cumulative_df = df if cumulative_df is None else cumulative_df.unionByName(df)
            continue

        logger.info(f"Traitement du fichier {file_path} pour la date {date_str}")
        df = spark.read.option("header", "true").option("escape", "\"").csv(file_path)
        for column in df.columns:
            if any(c in column for c in " ()%[].,;:{}"):
                new_column = column \
                    .replace("(", "_").replace(")", "_") \
                    .replace("%", "pct").replace(".", "_") \
                    .replace(" ", "_")
                df = df.withColumnRenamed(column, new_column)
        df = df.withColumn("situation_date", lit(dt.strftime("%Y-%m-%d")).cast("date"))
        cumulative_df = df if cumulative_df is None else cumulative_df.unionByName(df)

        if is_last_file:
            cumulative_df = cumulative_df.cache()
            final_path = os.path.join(output_path)
            logger.info(f"Écriture du fichier cumulé dans : {final_path}")
            cumulative_df.write.mode("overwrite").parquet(final_path)
            cumulative_df.unpersist()
        else:
            df = df.cache()
            df.write.mode("overwrite").parquet(output_path)
            df.unpersist()
            logger.info(f"Fichier écrit dans : {output_path}")

    spark.stop()
    logger.info("Traitement Spark terminé avec succès.")

if __name__ == "__main__":
    run_feeder()
