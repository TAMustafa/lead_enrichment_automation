# Lead Enrichment & Qualification Automation

A Python-based automation tool to enrich Salesforce Lead records with detailed business information and automatically qualify/disqualify them based on restaurant industry standards.

## Overview

This tool automates the gathering of business data (phone, website, ratings, service options) and applies a sophisticated qualification engine to filter leads. It uses the `simple-salesforce` library for CRM interaction and `serpapi` for Google Maps data extraction.

## Features

- **Automated Qualification**: Automatically marks leads as `Qualified` or `Disqualified` based on configurable business rules.
- **Restaurant Standards**:
    - **Residential Check**: Detects if an address is a private residence without a business listing.
    - **Service Verification**: Disqualifies Bars/Pubs/Cafes that do not offer delivery services.
    - **Operational Status**: Automatically disqualifies businesses marked as "Permanently Closed" or "Temporarily Closed" on Google Maps.
- **Configurable Rules**: All qualification logic is externalized in `qualification_config.json`, allowing non-technical users to update rules (keywords, categories, etc.) easily.
- **Selective Processing**: Targets only leads that haven't been qualified yet (`Qualification_Status__c = NULL`).
- **Data Normalization**: Parses complex Google Maps address strings into structured Salesforce fields (Street, City, Postal Code, Country).
- **Bulk Updates**: Uses the Salesforce Bulk API for high-efficiency record updates.

## Prerequisites

- **Python 3.12+**
- **Salesforce Custom Fields**: The following fields must exist on the Lead object:
    - `Google_Place_ID__c` (Text)
    - `Qualification_Status__c` (Picklist: Qualified, Disqualified)
    - `Disqualification_Reason__c` (Text/Long Text)
    - `Date_Enriched_At__c` (DateTime)
- **SerpAPI API Key**: For Google Maps search results.

## Setup

1.  **Clone and Install**:
    ```bash
    git clone <repository-url>
    cd "lead enrichment automation"
    uv sync  # or pip install -r requirements.txt
    ```

2.  **Configure Environment**:
    Create a `.env` file with your credentials:
    ```env
    SF_USERNAME=...
    SF_PASSWORD=...
    SF_TOKEN=...
    SERP_API=...
    ```

3.  **Adjust Rules**:
    Edit `qualification_config.json` to customize which business types or keywords trigger disqualification.

## Usage

Run the main automation script:
```bash
python lead_enrichment_automation.py
```
The script will fetch up to 10 leads (configurable), enrich them, apply qualification logic, and push the updates back to Salesforce.

## Project Structure

- `lead_enrichment_automation.py`: The main automation logic.
- `qualification_config.json`: Human-editable rules for the qualification engine.
- `LEARNING_RESOURCES.md`: Deep dive into the Python concepts used here.
