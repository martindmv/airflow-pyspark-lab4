"""
include/team_EYMARD_DEMERDJIEV_spark.py
"""
from __future__ import annotations

import json
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:
    """
    Transformation 1 - Lecture du Parquet silver avec schema explicite + filtre.
    Lit data/raw/dt=<logical_date>/ et cast les types.
    """
    schema = StructType([
        StructField("tx_id",       StringType(), True),
        StructField("category",    StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("country",     StringType(), True),
        StructField("amount_eur",  DoubleType(),  True),
        StructField("ts",   StringType(), True),
    ])

    silver_path = f"data/raw/dt={logical_date}/"
    df = spark.read.schema(schema).parquet(silver_path)

    # Filtre les lignes avec amount_eur manquant ou négatif
    df = df.filter(F.col("amount_eur") > 0)
    return df


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:
    """
    Transformation 2 - Enrichissement.
    Ajoute une colonne 'revenue' et la date logique.
    """
    df = df.withColumn("revenue", F.col("amount_eur"))
    df = df.withColumn("logical_date", F.lit(logical_date))
    return df


def transform_3(df: DataFrame) -> DataFrame:
    """
    Transformation 3 - Agrégation KPIs par category et country.
    Calcule le total revenue et le nombre de transactions.
    """
    df_kpi = df.groupBy("category", "country").agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("tx_id").alias("transaction_count"),
    )
    return df_kpi


def run_daily(logical_date: str, *, with_reference: bool = False) -> dict:
    """
    Appelé depuis le @task Airflow.
    Enchaîne transform_1 → transform_2 → transform_3, puis écrit les outputs.
    """
    spark = (
        SparkSession.builder
        .appName(f"kpi_pipeline_{logical_date}")
        .master("local[*]")
        .getOrCreate()
    )

    # Pipeline
    df_silver   = transform_1(spark, logical_date)
    df_enriched = transform_2(spark, df_silver, logical_date)
    df_kpi      = transform_3(df_enriched)

    # Écriture Parquet curated (idempotent grâce à overwrite)
    curated_path = f"curated/dt={logical_date}/"
    df_kpi.write.mode("overwrite").parquet(curated_path)

    # Calcul des totaux pour le JSON
    totals = df_enriched.agg(
        F.sum("revenue").alias("total_revenue"),
        F.count("tx_id").alias("total_transactions"),
    ).collect()[0]

    # Écriture du rapport JSON
    report_path = f"data/reports/dashboard_{logical_date}.json"
    os.makedirs("data/reports", exist_ok=True)
    report = {
        "status":             "ok",
        "logical_date":       logical_date,
        "total_revenue":      float(totals["total_revenue"]),
        "total_transactions": int(totals["total_transactions"]),
    }
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    spark.stop()
    return report