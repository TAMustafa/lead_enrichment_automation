# Lead Enrichment Automation

A Python-based automation tool to enrich Salesforce Lead records with detailed business information using the SerpAPI Google Maps engine.

## Overview

This tool automates the process of gathering business data (phone numbers, websites, ratings, reviews, and addresses) for leads that have a Google Place ID but are missing key attributes in Salesforce. It uses the `simple-salesforce` library for CRM interaction and `serpapi` for Google Maps data extraction.

## Features

- **Selective Enrichment**: Only processes leads that have a `Google_Place_ID__c` but are missing one or more key fields (Website, Store Type, Price Range, Google Rating, etc.).
- **SerpAPI Integration**: Leverages Google Maps data for accurate and up-to-date business information.
- **Bulk Updates**: Uses the Salesforce Bulk API to update records efficiently, minimizing API call usage.
- **Data Normalization**: Automatically parses and formats address components (Street, City, Postal Code, Country) from Google Maps strings.
- **Robust Logging**: Includes logging for tracking successes, failures, and API errors.

## Prerequisites

- **Python 3.12+**
- **Salesforce Account**: With API access and necessary custom fields (`Google_Place_ID__c`, `Store_Type__c`, etc.).
- **SerpAPI API Key**: For Google Maps search results.

## Setup

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd "lead enrichment automation"
    ```

2.  **Install dependencies**:
    Using `uv` (recommended):
    ```bash
    uv sync
    ```
    Or using `pip`:
    ```bash
    pip install google-search-results pandas python-dotenv simple-salesforce
    ```

3.  **Configure Environment Variables**:
    Create a `.env` file in the root directory with the following credentials:
    ```env
    SF_USERNAME=your_salesforce_username
    SF_PASSWORD=your_salesforce_password
    SF_TOKEN=your_salesforce_security_token
    SERP_API=your_serpapi_api_key
    ```

## Usage

1.  Open the `lead_automation.ipynb` notebook.
2.  Run the cells sequentially to:
    - Load configurations and validate credentials.
    - Connect to Salesforce and query leads for enrichment.
    - Process leads through SerpAPI.
    - Update Salesforce with the newly gathered data.

## Configuration

The query logic can be adjusted in the notebook to target different sets of leads or prioritize specific fields for enrichment. By default, it limits processing to 10 leads per run to ensure safe testing.
