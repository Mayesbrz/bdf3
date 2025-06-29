import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import get_logger
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, when, hour, dayofweek, month, trim, lower

def run_mltraining():
    logger = get_logger("mltraining")
    logger.info("Démarrage du MLTraining.")
    spark = SparkSession.builder \
        .appName("MLTraining") \
        .enableHiveSupport() \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "16") \
        .getOrCreate()

    accidents = spark.sql("SELECT * FROM silver.accidents")
    weather = spark.sql("SELECT * FROM silver.weather")
    road = spark.sql("SELECT * FROM silver.road_features")

    df = accidents.join(weather, "ID", "left").join(road, "ID", "left")

    if "Start_Time" in df.columns:
        df = df.withColumn("hour", hour(col("Start_Time")))
        df = df.withColumn("weekday", dayofweek(col("Start_Time")))
        df = df.withColumn("month", month(col("Start_Time")))

    for cat_col in ["Wind_Direction", "Weather_Condition"]:
        if cat_col in df.columns:
            indexer = StringIndexer(inputCol=cat_col, outputCol=f"{cat_col}_idx").setHandleInvalid("skip")
            df = indexer.fit(df).transform(df)

    features = [
        c for c in [
            "Start_Lat", "Start_Lng", "Amenity", "Bump", "Crossing", "Junction", "Traffic_Signal",
            "Temperature(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)", "Precipitation(in)",
            "hour", "weekday", "month", "Wind_Direction_idx", "Weather_Condition_idx"
        ] if c in df.columns
    ]
    if not features:
        logger.error("Aucune feature disponible pour l'entraînement.")
        spark.stop()
        return
    df = df.dropna(subset=["Severity"] + features)
    if df.count() == 0:
        logger.error("Aucune donnée d'entraînement disponible après filtrage.")
        spark.stop()
        return
    df = df.dropna()
    if df.count() == 0:
        logger.error("Aucune donnée d'entraînement disponible après suppression des valeurs nulles.")
        spark.stop()
        return
    indexer = StringIndexer(inputCol="Severity", outputCol="label")
    df = indexer.fit(df).transform(df)
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    df = assembler.transform(df)
    train, test = df.randomSplit([0.8, 0.2], seed=42)
   
    rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=20, maxBins=200)    
    model = rf.fit(train)
    predictions = model.transform(test)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    logger.info(f"Accuracy du modèle : {accuracy:.4f}")

    model.write().overwrite().save("ml_model_rf")
    logger.info("Modèle sauvegardé sous ml_model_rf")
    spark.stop()
    logger.info("Fin du MLTraining.")

def explore_silver_tables():
    spark = SparkSession.builder \
        .appName("ExploreSilver") \
        .enableHiveSupport() \
        .getOrCreate()
    print("\n--- Colonnes et exemples pour chaque table Silver ---")
    tables = ["accidents", "weather_conditions", "road_features", "light_conditions"]
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
    run_mltraining()
    explore_silver_tables()
