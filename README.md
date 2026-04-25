# Lead Enrichment & Qualification Automation

A Python-based automation tool that enriches Salesforce Lead records with Google Maps business data and automatically qualifies or disqualifies them using a configurable rule engine.

## 🚀 Key Features

- **Pre-Enrichment Filter**: Screens out junk and residential leads from Salesforce data *before* spending any API credits.
- **Google Maps Enrichment**: Fetches phone, website, rating, reviews, price range, service options, payment options, opening hours, and address components via SerpAPI.
- **Cuisine Categorisation**: Extracts and cleans Google Maps type tags into `Primary_Cuisine` and `Secondary_Cuisine` fields; falls back to website/menu scraping when types are too generic.
- **Fuzzy Name & Address Matching**: Uses `rapidfuzz` to tolerate minor spelling variations (e.g. "Totò's" → "Toto's") during business identity verification and FSA lookups.
- **Strict Qualification Engine**: Multi-step pipeline — closure status → address verification → name match → category blacklist → residential check → delivery requirement — each step producing an actionable disqualification reason.
- **UK FSA Hygiene Enrichment**: Fetches Food Standards Agency rating, inspection agency, and official URL for UK leads; results are cached per (name, postcode) pair to avoid duplicate HTTP calls.
- **International Market Support**: Supports **UK, NL, DE, AT, CH, BE, and US** via a `MarketFactory` / Strategy Pattern architecture.
- **Concurrent Processing**: `ThreadPoolExecutor` (10 workers) processes leads in parallel.
- **Salesforce Bulk Updates**: Writes enriched records back in batches of 2 000 using the Bulk API, with per-record failure reporting.

## 📁 Project Structure

| File | Role |
|------|------|
| `lead_enrichment_automation.py` | **Orchestrator** — Salesforce query, threading, merge logic, bulk update |
| `logic.py` | **Engine** — Market handlers, FSA integration, qualification rules, address parsing |
| `qualification_config.json` | **Ruleset** — Human-editable JSON controlling all qualification behaviour |

## 📋 Prerequisites

### Python
Python 3.12 or later.

### Salesforce Custom Fields

| Field API Name | Type | Notes |
|---|---|---|
| `Google_Place_ID__c` | Text | Used as the primary lookup key |
| `Qualification_Status__c` | Picklist | Values: `Qualified`, `Disqualified` |
| `Disqualification_Reason__c` | Text (255) | Populated on disqualification |
| `Store_Type__c` | Text | Raw Google Maps place types |
| `Price_Range__c` | Text | e.g. `$$` |
| `Google_Rating__c` | Number | 0–5 |
| `Google_Reviews__c` | Number | Review count |
| `Service_Options__c` | Text | e.g. `dine_in, delivery` |
| `Payment_Options__c` | Text | e.g. `credit_cards` |
| `Cuisine_Type__c` | Text | All detected cuisine types |
| `Primary_Cuisine__c` | Text | Top cuisine |
| `Secondary_Cuisine__c` | Text | Second cuisine |
| `Opening_Hours__c` | Long Text Area (32 768) | Compact formatted hours |
| `FSA_AGENCY__c` | Text | UK only — local authority name |
| `FSA_RATING__c` | Picklist | UK only — `ZERO` … `FIVE` |
| `FSA_URL__c` | URL | UK only — FSA business page |
| `Date_Enriched_At__c` | DateTime | Timestamp of last enrichment run |

### SerpAPI
A valid [SerpAPI](https://serpapi.com) key with Google Maps access.

## 🛠️ Setup & Usage

1. **Clone and install dependencies**:
    ```bash
    git clone <repository-url>
    cd "lead enrichment automation"
    uv sync
    ```

2. **Configure environment** — create a `.env` file:
    ```env
    SF_USERNAME=your_salesforce_username
    SF_PASSWORD=your_salesforce_password
    SF_TOKEN=your_salesforce_security_token
    SERP_API=your_serpapi_key
    ```

3. **Run**:
    ```bash
    python lead_enrichment_automation.py
    ```

The script queries leads where `Google_Place_ID__c != NULL AND Qualification_Status__c = NULL`, then applies a Python-side filter to skip records that already have all enrichment fields populated.

## ⚙️ Configuration (`qualification_config.json`)

All qualification behaviour is controlled by `rules` keys in `qualification_config.json`. No code changes are needed for day-to-day tuning.

| Key | Type | Effect |
|-----|------|--------|
| `require_delivery_for_types` | list[str] | Disqualify these place types unless delivery is enabled |
| `require_delivery_for_name_keywords` | list[str] | Same check triggered by business name keywords |
| `always_disqualify_types` | list[str] | Unconditionally disqualify these place types |
| `residential_signals` | list[str] | Google address component types that indicate a residential address |
| `residential_exception_types` | list[str] | Override residential disqualification for these types |
| `pre_qualification_rules.disqualify_name_keywords` | list[str] | Free pre-filter on Salesforce name/company |
| `pre_qualification_rules.disqualify_address_keywords` | list[str] | Free pre-filter on Salesforce street address |
| `cuisine_keywords` | list[str] | Keywords used by the menu-scraping cuisine detector |

## 🌍 Adding New Markets

The project uses the **Strategy Pattern**. To add a new market:

1. In `logic.py`, create a new handler inheriting from `MarketHandler`:
    ```python
    class FRMarketHandler(MarketHandler):
        def post_enrich(self, data, lead_row):
            # add France-specific enrichment here
            return data
    ```
2. Register it in `MarketFactory._MAPPING`:
    ```python
    FRMarketHandler: ["FR", "FRANCE"],
    ```
