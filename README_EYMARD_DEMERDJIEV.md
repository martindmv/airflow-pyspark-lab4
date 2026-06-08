# Team: <First Last> & <First Last>

**DAG id:** `team_EYMARD_DEMERDJIEV`  
**Git repo:** `https://github.com/martindmv/airflow-pyspark-lab4` - **also on your Moodle slides** (title or architecture)  
**Spark module:** `include/team_EYMARD_DEMERDJIEV_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

---

## 1. Business problem

<Who needs the dashboard? What breaks if the pipeline fails?>

**Defense tip:** for each section below, be ready to say **what you built** and **why** (not only that it runs).

**Submit by June 9, 23:59:** push capstone to **your pair's Git repo**; upload **slides on Moodle** with the **same URL** visible on the slides (title slide). Public repo, or private with instructor read access.

---

## 2. Architecture

<!-- Diagram: incoming → raw/dt= → curated/dt= → reports -->

| Layer | Path | Tool |
|-------|------|------|
| Bronze | `data/incoming/` | `vendor_drop.py` |
| Silver | `data/raw/dt=` | DuckDB (`ingest_day`) |
| Gold | `data/curated/dt=` | **Your** `team_EYMARD_DEMERDJIEV_spark.py` |
| Serve | `data/reports/` | JSON dashboard |

### Airflow (5 tasks)

| task_id | Role |
|---------|------|
| `wait_csv` | Attendre l'arrivée du fichier CSV du jour. |
| `ingest` | Ingérer les données brutes au format Parquet |
| `validate` | Valider les données Silver (échoue si les données sont corrompues) |
| `compute_kpis` | Lancer les transformations Spark (couche Gold) |
| `publish_report` | Une tâche finale créative (ex: log, notification, ou copie de JSON) |

**Dependency graph:**

```
`wait_csv` → `ingest` → `validate` → `compute_kpis` → `publish_report`
```

---

## 3. Spark transformations (≥3 - your code)

File: `include/team_EYMARD_DEMERDJIEV_spark.py`

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | `Lecture du Parquet silver avec schema explicite + filtre. Lit data/raw/dt=<logical_date>/ et cast les types.` |
| 2 | `transform_2` | `Enrichissement. Ajoute une colonne 'revenue' et la date logique.` |
| 3 | `transform_3` | `Agrégation KPIs par category et country. Calcule le total revenue et le nombre de transactions.` |

---

## 4. Idempotence

<Re-run same `ds`: what gets overwritten under `raw/dt=`, `curated/dt=`, `dashboard_*.json`?>

---

## 5. Backfill

```bash
docker compose exec airflow-scheduler \
  airflow dags backfill team_EYMARD_DEMERDJIEV -s 2026-06-01 -e 2026-06-07 --reset-dagruns
```

---

## 6. Failure demo

```bash
python scripts/vendor_drop.py --date 2026-06-03 --corrupt
```

<Which task fails? What appears in the Airflow UI?>

---

## 7. Exploration tracks

| Track | Done? | Describe your implementation |
|-------|-------|----------|
| R Reliability | | |
| S Spark depth | | |
| O Orchestration | YES | Created a corrupted file and triggered DAG to see that it fails. |
| Q Data quality | | |
| P Custom | | |
| X SparkSubmit | | |

---

## 8. Demo script & backup

---

## 9. Production next steps

