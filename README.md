# Anna Kozina - Data & AI Engineering Portfolio

This repository contains curated code samples from my personal projects, highlighting my skills in data engineering, machine learning, and full-stack development.
## Project 1: GrowingUpChicago.org - Automated Event Data Platform

Live Site: https://www.growingupchicago.org
### Project Summary

I architected and built an end-to-end data platform that automatically aggregates, normalizes, and serves event data for families in Chicago. The goal was to create a single, reliable source for local family-friendly activities.
High-Level Architecture

[APIs / Web Scrapers] -> [Airflow for Orchestration] -> [Python for Transformation/Normalization] -> [PostgreSQL DB] -> [API] -> [Next.js Frontend]
Technology Stack

    Orchestration: Apache Airflow

    Data Processing: Python (Pandas), NLP (for normalization)

    Infrastructure: Docker

    Web Scraping: Playwright, BeautifulSoup

    Database: PostgreSQL

Code Showcase

Below are the core components of the data pipeline.
### 1. pipeline_DAG.py - The Airflow Orchestrator

This file defines the Airflow DAG that schedules and manages the entire ETL process. It demonstrates my ability to build reliable, scheduled data workflows.

Key Responsibilities:

    Schedules & Automates: Triggers the pipeline to run on a daily schedule, managing the entire workflow from start to finish.

    Manages Dependencies: Defines the precise sequence of tasks (Extract -> Transform -> Load) and handles the flow of data between them using XComs.

    Ensures Resilience: Implements production-grade features like automated retries and failure notifications.


### 2. pipeline.py - The Core ETL Logic

This script contains the main functions called by the Airflow DAG. It handles the logic for scraping data from various sources.

Key Responsibilities:
    
    Multi-Source Ingestion (extract_sources): Implements strategies for pulling data from a wide variety of sources, including REST APIs, CSV endpoints, and direct web scraping (using helpers).

    Intelligent Transformation (transform_records): Orchestrates the normalization process by batching raw data by its source type and passing it to the EventTransformer.

    Data Quality Assurance (post_process_records): Performs critical cross-source fuzzy deduplication to merge duplicate events and uses an LLM-powered PriceCategorizer to standardize pricing information.

    Efficient Loading (load_to_supabase): Manages the database connection and performs an efficient "upsert" operation to load the clean data into a Supabase (PostgreSQL) database, including table cleanup and relationship linking.

### 3. events_transformer.py - AI-Powered Data Normalization

This is the heart of the project's "smarts." This class takes the raw, messy data from the pipeline and uses rules-based logic and NLP models to standardize it.

Key Responsibilities:

    Hybrid Transformation Strategy:

        - For structured APIs (_transform_dataframe_events): Applies predefined mappings for efficiency and reliability.

        - For unstructured web scrapes (_transform_chicagoparent_events): Leverages an LLM to parse raw text and HTML.

    AI-Powered Entity Extraction: Uses prompt engineering to instruct the Gemini model to act as a JSON-based parser, reliably extracting entities like min/max age, categories, price, and event_date from free-form text.

    Dynamic AI Mapping (_get_ai_category_mapping): Instead of hard-coding hundreds of rules, it dynamically queries the LLM to build a mapping from source-specific categories (e.g., "Crafty Kids") to a standardized set (e.g., "Arts & Crafts").

    Object-Oriented Design: Encapsulates all transformation logic within a clean, configurable class, making the system easy to maintain and test.

    
