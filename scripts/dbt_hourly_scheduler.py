import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
DBT_PROJECT_PATH = r"PROJECT PATH"
DBT_PROFILE_DIR = r"DBT Profile directory"  # <-- Update this to your actual profiles.yml folder
PYTHON_SCRIPT_PATH = r"Script path for rss_sentiment.py to call inside the script"

# -----------------------------
# Model Sequences
# -----------------------------
staging_models = [
    "rss_base",
    "rss_clean",
    "dim_rss_source",
    "dim_rss_date",
    "fact_rss_events",
    "fact_rss_enriched"
]

analytical_models = [
    "fact_sentiment_trends",
    "fact_source_sentiment",
    "fact_source_sentiment_vw",
    "fact_sentiment_overall"
]

# -----------------------------
# Function to run dbt model sequence
# -----------------------------
def run_dbt_sequence(models_list):
    for model in models_list:
        print(f"[{datetime.now()}] 🚀 Starting dbt run for model: {model}")
        result_run = subprocess.run(
            [
                "dbt", "run",
                "--models", model,
                "--target", "prod",
                "--profile", "scheduler",
                "--profiles-dir", DBT_PROFILE_DIR
            ],
            cwd=DBT_PROJECT_PATH,
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        if result_run.returncode != 0:
            print(f"[{datetime.now()}] ❌ dbt run FAILED for model: {model}")
            return False
        print(f"[{datetime.now()}] ✅ dbt run completed for model: {model}")

        print(f"[{datetime.now()}] Starting dbt test for model: {model}")
        result_test = subprocess.run(
            [
                "dbt", "test",
                "--models", model,
                "--target", "prod",
                "--profile", "scheduler",
                "--profiles-dir", DBT_PROFILE_DIR
            ],
            cwd=DBT_PROJECT_PATH,
            stdout=sys.stdout,
            stderr=sys.stderr
        )
        if result_test.returncode != 0:
            print(f"[{datetime.now()}] ⚠️ dbt test FAILED for model: {model}")
            return False
        print(f"[{datetime.now()}] ✅ dbt test completed for model: {model}\n")
    return True

# -----------------------------
# Function to run Python sentiment script
# -----------------------------
def run_sentiment_script():
    print(f"[{datetime.now()}] 🧠 Running sentiment analysis Python script...")
    result = subprocess.run(
        ["python", PYTHON_SCRIPT_PATH],
        stdout=sys.stdout,
        stderr=sys.stderr
    )
    if result.returncode != 0:
        print(f"[{datetime.now()}] ❌ Sentiment script failed!")
        return False
    print(f"[{datetime.now()}] ✅ Sentiment analysis completed successfully.\n")
    return True

# -----------------------------
# MAIN LOOP: Run hourly
# -----------------------------
print(f"[{datetime.now()}] Starting dbt + sentiment hourly scheduler (foreground)...")

while True:
    print(f"[{datetime.now()}] ⏱️ Starting new hourly cycle...\n")

    if not run_dbt_sequence(staging_models):
        print(f"[{datetime.now()}] ❌ Stopping this cycle due to failure in staging/enrichment models.\n")
        time.sleep(3600)
        continue

    if not run_sentiment_script():
        print(f"[{datetime.now()}] ❌ Stopping this cycle due to Python sentiment failure.\n")
        time.sleep(3600)
        continue

    if not run_dbt_sequence(analytical_models):
        print(f"[{datetime.now()}] ❌ Stopping this cycle due to failure in analytical models.\n")
        time.sleep(3600)
        continue

    print(f"[{datetime.now()}] ✅ Hourly cycle completed successfully.\n")
    print(f"[{datetime.now()}] Waiting 1 hour until next run...\n")
    time.sleep(3600)
