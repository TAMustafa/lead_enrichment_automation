# Lead Enrichment & Qualification Automation

A professional Python-based automation tool designed to enrich Salesforce Lead records with detailed business information and automatically qualify or disqualify them using a rule-based engine.

## 🌟 New Features: Multi-Market & UK Hygiene

- **International Market Support**: Now supports **UK, NL, DE, AT, CH, and BE** with a scalable Market Handler architecture.
- **UK Food Hygiene Enrichment**: Automatically fetches **FSA (Food Standards Agency)** ratings, inspection agencies, and official URLs for UK leads.
- **Strict Location Verification**: Uses a "Ground Truth" check comparing Salesforce address data with Google results to prevent brand mix-ups.
- **Smart Name Normalization**: Handles accents and special characters (e.g., "Totò's" matches "Toto's") to improve identification accuracy.

## 🚀 Key Features

- **Automated Qualification Engine**: Categorizes leads as `Qualified` or `Disqualified` based on industry standards.
- **Cost Optimization**: Includes a "Pre-Enrichment" layer that filters out junk or residential leads *before* making any paid API calls.
- **Modular Factory Architecture**: 
    - **Market Specificity**: Uses a `MarketFactory` to apply custom enrichment logic per country.
    - **Logic Separation**: Parsing and qualification rules are separated from the main execution script.
- **Restaurant-Specific Logic**: Detects delivery services, residential addresses, and business closure statuses.
- **Salesforce Bulk Integration**: Efficiently updates records in batches, minimizing CRM overhead.

## 📁 Project Structure

- **`lead_enrichment_automation.py`**: The **Orchestrator**. Handles Salesforce connections and the main processing loop.
- **`logic.py`**: The **Brain**. Contains Market Handlers, FSA integration, and strict address verification logic.
- **`qualification_config.json`**: The **Ruleset**. A human-editable file where you define categories, keywords, and residential signals.

## 📋 Prerequisites

- **Python 3.12+**
- **Salesforce Custom Fields**:
    - `Google_Place_ID__c` (Text)
    - `Qualification_Status__c` (Picklist: Qualified, Disqualified)
    - `FSA_AGENCY__c` (Text)
    - `FSA_RATING__c` (Picklist: ZERO, ONE, TWO, THREE, FOUR, FIVE)
    - `FSA_URL__c` (URL)
- **SerpAPI API Key**: For Google Maps data extraction.

## 🛠️ Setup & Usage

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

## 🌍 Scalability: Adding New Markets

The project uses a **Strategy Pattern**. To add a new market (e.g., Germany):
1.  In `logic.py`, create a new `DEMarketHandler` inheriting from `MarketHandler`.
2.  Add your country-specific API logic (like the FSA logic in the UK handler).
3.  Register the handler in the `MarketFactory._MAPPING` dictionary.
