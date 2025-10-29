# RSS Sentiment Analysis – Real-Time AI Feed Analytics
#Overview

This project demonstrates an end-to-end, real-time data engineering and machine learning pipeline that collects, processes, and analyzes RSS feed data related to Artificial Intelligence (AI) and emerging technology topics.
It combines data ingestion, transformation, machine learning, and visualization into a unified, production style architecture using modern data tools such as RabbitMQ, GCP Cloud Storage, Snowflake, dbt, and Hugging Face Transformers.

#Project Goals

- Collect and process AI-related articles from sources like Medium, OpenAI, and Google AI.
- Perform sentiment analysis on article titles and summaries.
- Deliver real-time dashboards for AI media sentiment and topic visibility.
- Showcase data engineering + machine learning + BI integration in one architecture.

#Architecture Overview
End-to-End Data Flow:

RSS Feeds → RabbitMQ → GCP Cloud Storage → Snowflake (Snowpipe)
           → dbt Transformations → Python (DistilBERT)
           → Snowflake (Predictions) → Looker Studio Dashboard

Architecture Highlights:

- Decoupled ingestion using RabbitMQ for fault tolerance.
- Snowpipe for continuous, auto-ingested data loading.
- dbt for incremental transformations and quality checks.
- Python (Transformers) for ML-based sentiment classification.
- Looker Studio for near-real-time insights and visualization.
See architecture diagram in /docs/RSS_Sentiment_Architecture.png.


