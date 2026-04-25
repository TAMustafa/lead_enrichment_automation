# Lead Enrichment & Qualification Automation

A professional Python-based automation tool designed to enrich Salesforce Lead records with detailed business information and automatically qualify or disqualify them using a rule-based engine.

## Overview

This tool automates the process of gathering business data (phone, website, ratings, service options) and applies a sophisticated qualification logic to filter leads. It uses a **modular architecture** to separate business logic from the automation workflow.

## Features

- **Automated Qualification Engine**: Categorizes leads as `Qualified` or `Disqualified` based on industry standards.
- **Cost Optimization**: Includes a "Pre-Enrichment" layer that filters out junk or residential leads *before* making any paid API calls.
- **Modular Architecture**: 
    - **Logic Separation**: Parsing and qualification rules are separated from the main execution script.
    - **External Configuration**: Rules are managed via a simple JSON file.
- **Restaurant-Specific Logic**: Detects delivery services, residential addresses, and closed business statuses.
- **Salesforce Bulk Integration**: Efficiently updates records in batches, minimizing CRM overhead.

## Project Structure

- **`lead_enrichment_automation.py`**: The **Orchestrator**. Handles environment setup, Salesforce connections, SerpAPI calls, and the main processing loop.
- **`logic.py`**: The **Brain**. Contains all data parsing functions, qualification algorithms, and the pre-enrichment filter.
- **`qualification_config.json`**: The **Ruleset**. A human-editable file where you define categories, keywords, and residential signals.
- **`LEARNING_RESOURCES.md`**: A guide explaining the advanced Python concepts used in this project.

## Prerequisites

- **Python 3.12+**
- **Salesforce Custom Fields**:
    - `Google_Place_ID__c` (Text)
    - `Qualification_Status__c` (Picklist: Qualified, Disqualified)
    - `Disqualification_Reason__c` (Text/Long Text)
    - `Date_Enriched_At__c` (DateTime)
- **SerpAPI API Key**: For Google Maps data extraction.

## Setup & Usage

1.  **Clone and Install**:
    ```bash
    git clone <repository-url>
    cd "lead enrichment automation"
    uv sync  # or pip install -r requirements.txt
    ```

2.  **Configure Environment**:
    Add your credentials to a `.env` file:
    ```env
    SF_USERNAME=...
    SF_PASSWORD=...
    SF_TOKEN=...
    SERP_API=...
    ```

3.  **Run Automation**:
    ```bash
    python lead_enrichment_automation.py
    ```

## Customizing Rules

You can modify the qualification logic without touching any Python code by editing `qualification_config.json`. You can add blacklisted types, required delivery keywords, or residential address indicators.
