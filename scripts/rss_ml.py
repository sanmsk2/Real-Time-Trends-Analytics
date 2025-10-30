"""
RSS Sentiment Analysis – Full End-to-End ML with Snowflake Write-Back
Author: ChatGPT (CPU-safe, chunked DistilBERT)

Upgraded Features:
- Processes dataset in chunks to reduce RAM usage
- CPU-only execution to prevent WinError 1114
- Truncates FACT_RSS_PREDICTIONS before writing new results
- Ensures column names are quoted for Snowflake compatibility
"""

import os
os.environ["CUDA_VISIBLE_DEVICES"] = ""  # Force CPU

import pandas as pd
import re
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from transformers import pipeline
from snowflake.connector.pandas_tools import write_pandas

# ---------- Snowflake Connection ----------
key_path = r"rsa_key.p8"
with open(key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(key_file.read(), password=None)

conn = snowflake.connector.connect(
    user="Username",
    account="Snowflake Account Name",
    private_key=private_key,
    authenticator="snowflake_jwt",
    warehouse="RSS_WH",
    database="RSS_DB",
    schema="RSS_SCH",
    client_session_keep_alive=True,
    insecure_mode=True
)

# ---------- Fetch Full Dataset ----------
query = """
SELECT EVENT_ID, TITLE, SUMMARY, SOURCE_ID, PUBLISHED_AT
FROM FACT_RSS_ENRICHED LIMIT 5000;
"""
df = pd.read_sql(query, conn)
df.columns = [c.lower() for c in df.columns]

# ---------- Combine and Clean Text ----------
df["text"] = df["title"].fillna('') + ' ' + df["summary"].fillna('')

def clean_text(text):
    text = text.lower()
    text = re.sub(r"http\S+|www\S+", "", text)
    text = re.sub(r"[^a-z\s]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

df["clean_text"] = df["text"].apply(clean_text)

# ---------- Load DistilBERT Sentiment Pipeline ----------
print("Loading DistilBERT sentiment model (CPU only)...")
sentiment_model = pipeline("sentiment-analysis", device=-1)  # CPU

# ---------- Predict Sentiment in Chunks ----------
print("Predicting sentiments using DistilBERT (chunked)...")
chunk_size = 500
all_results = []

for start in range(0, len(df), chunk_size):
    end = start + chunk_size
    chunk_texts = df["clean_text"].iloc[start:end].tolist()
    chunk_results = sentiment_model(chunk_texts, truncation=True, batch_size=16)
    all_results.extend(chunk_results)
    print(f"Processed rows {start} to {min(end, len(df))}")

# ---------- Map Predictions ----------
labels = [r["label"].upper() for r in all_results]
scores = [r["score"] for r in all_results]

label_map = {"POSITIVE": 1, "NEGATIVE": -1, "NEUTRAL": 0}
df["PREDICTED_LABEL"] = [label_map.get(lbl, 0) for lbl in labels]
df["PREDICTED_SENTIMENT"] = labels
df["CONFIDENCE"] = scores

# ---------- Prepare DataFrame for Snowflake ----------
df_pred = df[[
    "event_id",
    "title",
    "summary",
    "source_id",
    "published_at",
    "PREDICTED_LABEL",
    "PREDICTED_SENTIMENT",
    "CONFIDENCE"
]].copy()

# ---------- Truncate existing table ----------
print("Truncating FACT_RSS_PREDICTIONS before inserting new results...")
conn.cursor().execute("TRUNCATE TABLE FACT_RSS_PREDICTIONS")

# ---------- Write Predictions Back to Snowflake ----------
success, nchunks, nrows, _ = write_pandas(
    conn,
    df_pred,
    "FACT_RSS_PREDICTIONS",
    quote_identifiers=False  # ensures exact column names are preserved
)
print(f"✅ Snowflake write success: {success}, rows written: {nrows}")

conn.close()

# ---------- Example Predictions ----------
examples = [
    "The company achieved record profits this quarter.",
    "The company did not achieve record profits this quarter.",
    "Neutral update with no major impact."
]

example_results = sentiment_model(examples, truncation=True, batch_size=8)
print("\nExample Predictions:")
for text, r in zip(examples, example_results):
    print(f"Input: {text}")
    print(f"Predicted Sentiment: {r['label']} (Confidence: {r['score']:.2f})")

print("\n🎯 Full RSS Sentiment Analysis Completed with Snowflake Write-Back (CPU-safe, chunked)!")
