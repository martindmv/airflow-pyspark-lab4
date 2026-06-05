"""
Lab 4 - copy to dags/team_EYMARD_DEMERDJIEV.py and complete the capstone.

Mandatory:
  - >= 5 Airflow tasks in your dag
  - 3 Spark transforms in include/team_EYMARD_DEMERDJIEV_spark.py
  - Try to be creative with the tasks

Steps:
  1. Change dag_id below.
  2. Copy include/team_spark_TEMPLATE.py -> include/team_EYMARD_DEMERDJIEV_spark.py
  3. Define 5 tasks
  4. Wire spark task to YOUR run_daily() in include/team_EYMARD_DEMERDJIEV_spark.py
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.filesystem import FileSensor

from include.ingest import ingest_day, validate_silver
from include.paths import report_json

# TODO: after creating team_EYMARD_DEMERDJIEV_spark.py, import run_daily from there:
# from include.team_EYMARD_DEMERDJIEV_spark import run_daily

DEFAULT_ARGS = {
    "owner": "team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


with DAG(
    dag_id="team_EYMARD_DEMERDJIEV",
    description="Capstone retail KPI pipeline",
    start_date=datetime(2026, 6, 1),
    end_date=datetime(2026, 6, 14),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["lab4", "capstone"],
) as dag:

    # Tâche 1 : Attendre l'arrivée du fichier CSV du jour [cite: 34]
    wait_csv = FileSensor(
        task_id="wait_for_daily_csv",
        filepath="data/incoming/transactions_{{ ds }}.csv", # {{ ds }} est la date logique d'Airflow
        mode="poke",
        timeout=600
    )

    # Tâche 2 : Ingérer les données brutes au format Parquet
    @task
    def ingest(ds=None):
        ingest_day(ds)

    # Tâche 3 : Valider les données Silver (échoue si les données sont corrompues) [cite: 50]
    @task
    def validate(ds=None):
        validate_silver(ds)

    # Tâche 4 : Lancer les transformations Spark (couche Gold)
    @task
    def compute_kpis(ds=None):
        # run_daily(ds) # À décommenter quand la partie Spark sera codée
        print(f"Lancement de Spark pour la date : {ds}")

    # Tâche 5 : Une tâche finale créative (ex: log, notification, ou copie de JSON) [cite: 54]
    @task
    def publish_report(ds=None):
        print(f"Le tableau de bord pour {ds} est prêt et publié !")

    # --- Définition des dépendances (Le câblage du graphe) ---
    wait_csv >> ingest() >> validate() >> compute_kpis() >> publish_report()


