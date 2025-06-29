import sys
import os
import shutil
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import get_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime

def run_preprocessor():
    logger = get_logger("preprocessor")
    logger.info("Démarrage du preprocessor.")
    spark = SparkSession.builder \
        .appName("Preprocessor") \
        .enableHiveSupport() \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

    bronze_base = "../Bronze" if not os.path.exists("Bronze") else "Bronze"
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")

    warehouse_path = os.path.join(os.getcwd(), "spark-warehouse", "silver.db")
    for table in ["accidents", "weather", "road_features"]:
        spark.sql(f"DROP TABLE IF EXISTS silver.{table}")
        logger.info(f"Table silver.{table} supprimée si elle existait.")
        table_path = os.path.join(warehouse_path, table)
        if os.path.exists(table_path):
            shutil.rmtree(table_path)
            logger.info(f"Dossier physique {table_path} supprimé.")
    for year in os.listdir(bronze_base):
        year_path = os.path.join(bronze_base, year)
        if not os.path.isdir(year_path):
            continue
        for month in os.listdir(year_path):
            month_path = os.path.join(year_path, month)
            if not os.path.isdir(month_path):
                continue
            for day in os.listdir(month_path):
                day_path = os.path.join(month_path, day)
                if not os.path.isdir(day_path):
                    continue
                logger.info(f"Traitement : {day_path}")
                df = spark.read.parquet(day_path)
                if 'ID' in df.columns:
                    df = df.filter(col('ID').isNotNull())
                from pyspark.sql.functions import when, trim, lower, lit
                accidents_cols = [
                    'ID', 'Severity', 'Start_Time', 'End_Time', 'Start_Lat', 'Start_Lng',
                    'Description', 'City', 'County', 'State', 'Timezone'
                ]
                for colname in accidents_cols:
                    if colname not in df.columns:
                        if colname in ['Start_Lat', 'Start_Lng']:
                            df = df.withColumn(colname, lit(None).cast('float'))
                        elif colname == 'Severity':
                            df = df.withColumn(colname, lit(None).cast('int'))
                        else:
                            df = df.withColumn(colname, lit(None).cast('string'))
                accidents_df = df.select(accidents_cols)
                for colname in ['Start_Lat', 'Start_Lng']:
                    if colname in accidents_df.columns:
                        accidents_df = accidents_df.withColumn(colname, col(colname).cast('float'))
                for colname in ['City', 'County', 'State', 'Timezone']:
                    if colname in accidents_df.columns:
                        accidents_df = accidents_df.withColumn(colname, lower(trim(col(colname))))
                if 'Start_Time' in accidents_df.columns and 'End_Time' in accidents_df.columns:
                    accidents_df = accidents_df.filter(col('Start_Time') < col('End_Time'))
                if 'Severity' in accidents_df.columns:
                    accidents_df = accidents_df.filter(col('Severity').isin([1,2,3,4]))
                if 'ID' in accidents_df.columns:
                    accidents_df = accidents_df.dropDuplicates(['ID'])
                accidents_df = accidents_df.dropna()
                accidents_df = accidents_df.repartition(8)
                accidents_df = accidents_df.persist()
                silver_accidents_path = os.path.join("Silver", "accidents", year, month, day)
                accidents_df.write.mode("overwrite").parquet(silver_accidents_path)
                logger.info(f"Écriture Parquet dans {silver_accidents_path}")
                accidents_df.write.mode("append").format("hive").saveAsTable("silver.accidents")
                logger.info(f"Écriture dans Hive : silver.accidents")
                accidents_df.unpersist()

                weather_cols = [
                    'ID', 'Weather_Timestamp', 'Temperature(F)', 'Humidity(%)', 'Pressure(in)',
                    'Visibility(mi)', 'Wind_Direction', 'Wind_Speed(mph)', 'Precipitation(in)', 'Weather_Condition'
                ]
                for colname in weather_cols:
                    if colname not in df.columns:
                        if colname in ['Temperature(F)', 'Humidity(%)', 'Pressure(in)', 'Visibility(mi)', 'Wind_Speed(mph)', 'Precipitation(in)']:
                            df = df.withColumn(colname, lit(None).cast('float'))
                        else:
                            df = df.withColumn(colname, lit(None).cast('string'))
                weather_df = df.select(weather_cols)
                for colname in ['Wind_Direction', 'Weather_Condition']:
                    if colname in weather_df.columns:
                        weather_df = weather_df.withColumn(colname, lower(trim(col(colname))))
                if 'ID' in weather_df.columns:
                    weather_df = weather_df.dropDuplicates(['ID'])
                weather_df = weather_df.dropna()
                weather_df = weather_df.repartition(8)
                weather_df = weather_df.persist()
                silver_weather_path = os.path.join("Silver", "weather", year, month, day)
                weather_df.write.mode("overwrite").parquet(silver_weather_path)
                logger.info(f"Écriture Parquet dans {silver_weather_path}")
                weather_df.write.mode("append").format("hive").saveAsTable("silver.weather")
                logger.info(f"Écriture dans Hive : silver.weather")
                weather_df.unpersist()

                road_cols = [
                    'ID', 'Amenity', 'Bump', 'Crossing', 'Junction', 'Traffic_Signal'
                ]
                for colname in road_cols:
                    if colname not in df.columns:
                        if colname == 'ID':
                            df = df.withColumn(colname, lit(None).cast('string'))
                        else:
                            df = df.withColumn(colname, lit(None).cast('int'))
                road_df = df.select(road_cols)
                for colname in ['Amenity', 'Bump', 'Crossing', 'Junction', 'Traffic_Signal']:
                    if colname in road_df.columns:
                        road_df = road_df.withColumn(colname, when(col(colname) == "True", 1).when(col(colname) == "False", 0).otherwise(None))
                if 'ID' in road_df.columns:
                    road_df = road_df.dropDuplicates(['ID'])
                road_df = road_df.dropna()
                road_df = road_df.repartition(8)
                road_df = road_df.persist()
                silver_road_path = os.path.join("Silver", "road_features", year, month, day)
                road_df.write.mode("overwrite").parquet(silver_road_path)
                logger.info(f"Écriture Parquet dans {silver_road_path}")
                road_df.write.mode("append").format("hive").saveAsTable("silver.road_features")
                logger.info(f"Écriture dans Hive : silver.road_features")
                road_df.unpersist()
    spark.stop()
    logger.info("Fin du preprocessor.")

    spark = SparkSession.builder \
        .appName("ExportHiveToParquet") \
        .enableHiveSupport() \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    for table in ["accidents", "weather", "road_features"]:
        try:
            df = spark.sql(f"SELECT * FROM silver.{table}")
            export_path = os.path.join("Silver", table, "all_data")
            df.write.mode("overwrite").parquet(export_path)
            logger.info(f"Export Hive {table} -> {export_path}")
        except Exception as e:
            logger.error(f"Erreur export Hive {table} : {e}")
    spark.stop()

def check_hive_tables():
    spark = SparkSession.builder \
        .appName("CheckHiveTables") \
        .enableHiveSupport() \
        .getOrCreate()
    print("Tables dans la base Hive 'silver':")
    tables = spark.sql("SHOW TABLES IN silver")
    tables.show(truncate=False)
    for table in ["accidents", "weather", "road_features"]:
        print(f"\nAperçu de silver.{table} :")
        try:
            df = spark.sql(f"SELECT * FROM silver.{table} LIMIT 5")
            df.show(truncate=False)
        except Exception as e:
            print(f"Erreur lors de la lecture de {table}: {e}")
    spark.stop()

def explore_silver_tables():
    spark = SparkSession.builder \
        .appName("ExploreSilver") \
        .enableHiveSupport() \
        .getOrCreate()
    print("\n--- Colonnes et exemples pour chaque table Silver ---")
    tables = ["accidents", "weather", "road_features"]
    for table in tables:
        print(f"\nTable: silver.{table}")
        df = spark.sql(f"SELECT * FROM silver.{table}")
        print("Colonnes:", df.columns)
        df.show(5, truncate=False)
        for colname in df.columns:
            null_count = df.filter(df[colname].isNull()).count()
            print(f"  {colname}: {null_count} valeurs nulles")
    spark.stop()

if __name__ == "__main__":
    run_preprocessor()
    check_hive_tables()
    explore_silver_tables()
