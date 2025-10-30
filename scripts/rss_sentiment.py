import pandas as pd
from textblob import TextBlob
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization

# -------------------------------
# 1. Load private key for Snowflake
# -------------------------------
key_path = r"rsa_key.p8"
with open(key_path, "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None  # use b"your_passphrase" if key is encrypted
    )

# -------------------------------
# 2. Connect to Snowflake
# -------------------------------
conn = snowflake.connector.connect(
    user="Username",
    account="Snowflake Account Name",
    private_key=private_key,
    authenticator="snowflake_jwt",
    warehouse="RSS_WH",
    database="RSS_DB",
    schema="RSS_SCH",
    client_session_keep_alive=True,
    insecure_mode=True  # temporary to bypass OCSP issues
)

# -------------------------------
# 3. Read base enriched table
# -------------------------------
query = "SELECT * FROM fact_rss_enriched"
df = pd.read_sql(query, conn)

print("✅ Data loaded from Snowflake")
print("Columns returned:", df.columns.tolist())
print(df.head(3))

# -------------------------------
# 4. Calculate sentiment
# -------------------------------
def sentiment_score(text):
    if not isinstance(text, str):
        return 0
    return TextBlob(text).sentiment.polarity

def sentiment_label(score):
    if score > 0.1:
        return "positive"
    elif score < -0.1:
        return "negative"
    else:
        return "neutral"

# Handle case differences automatically
title_col = next((col for col in df.columns if col.lower() == "title"), None)
summary_col = next((col for col in df.columns if col.lower() == "summary"), None)

if not title_col or not summary_col:
    raise KeyError("❌ Could not find 'title' or 'summary' columns in the dataframe.")

df['title_sentiment_score'] = df[title_col].apply(sentiment_score)
df['summary_sentiment_score'] = df[summary_col].apply(sentiment_score)
df['title_sentiment_label'] = df['title_sentiment_score'].apply(sentiment_label)
df['summary_sentiment_label'] = df['summary_sentiment_score'].apply(sentiment_label)

print("✅ Sentiment analysis complete")

# -------------------------------
# 5. Write results back to Snowflake
# -------------------------------
success, nchunks, nrows, _ = write_pandas(
    conn, df, 'FACT_RSS_SENTIMENT', schema='RSS_SCH', overwrite=True
)

if success:
    print(f"✅ Inserted {nrows} rows into RSS_SCH.FACT_RSS_SENTIMENT")
else:
    print("❌ Failed to write data to Snowflake")

conn.close()
print("✅ Connection closed")
