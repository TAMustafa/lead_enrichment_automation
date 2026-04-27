# Lead Enrichment & Qualification Automation

A Python-based automation tool that enriches Salesforce Lead records with Google Maps business data and automatically qualifies or disqualifies them using a configurable rule engine.

## 🚀 Key Features

- **Pre-Enrichment Filter**: Screens out junk and residential leads from Salesforce data *before* spending any API credits.
- **Google Maps Enrichment**: Fetches phone, website, rating, reviews, price range, service options, payment options, opening hours, and address components via SerpAPI.
- **E.164 Phone Normalization**: Normalizes enriched phone numbers to international E.164 format (`+442079460958`) using `phonenumbers`, falling back gracefully on unparseable values.
- **Cuisine Categorisation**: Uses a multi-stage cuisine detector that prioritizes explicit cuisine types from Google Maps and falls back to menu/website text analysis when types are generic.
- **Fuzzy Name & Address Matching**: Uses `rapidfuzz` to tolerate minor spelling variations (e.g. "Totò's" → "Toto's") during business identity verification and FSA lookups.
- **Strict Qualification Engine**: Multi-step pipeline — closure status → address verification → name match → category blacklist → residential check → delivery requirement — each step producing an actionable disqualification reason.
- **UK FSA Hygiene Enrichment**: Fetches Food Standards Agency rating, inspection agency, and official URL for UK leads with retry + cache + fuzzy-confidence thresholding.
- **Competitor URL Enrichment**: Detects Uber Eats and Deliveroo listing URLs via DuckDuckGo `site:` queries, with market-aware platform mapping.
- **International Market Support**: Supports **UK, NL, DE, AT, CH, BE, and US** via a `MarketFactory` / Strategy Pattern architecture.
- **Concurrent Processing**: `ThreadPoolExecutor` (10 workers) processes leads in parallel.
- **Salesforce Bulk Updates**: Writes enriched records back in batches of 2 000 using the Bulk API, with per-record failure reporting.

## 🔍 How Enrichment Works

The pipeline for each lead is:

1. **Pre-qualification** (`pre_qualify_lead`):
   - Filters obvious junk/residential leads from Salesforce-native data.
   - Saves SerpAPI and external HTTP usage when the lead is clearly invalid.

2. **Google Maps enrichment** (`MarketHandler.enrich`):
   - **Strategy A (preferred):** place ID lookup using `Google_Place_ID__c`.
   - **Strategy B (fallback):** text query with name + address context.
   - If only `local_results` are returned, the best fuzzy name match is selected.
   - If that local result includes `place_id`/`data_id`, the script upgrades to a full place-details call.

3. **Market-specific post enrichment** (`post_enrich`):
   - All markets: competitor URL search.
   - UK: additional FSA hygiene enrichment.

4. **Qualification evaluation** (`evaluate_qualification`):
   - Closed status checks.
   - Address verification against Salesforce street/postcode.
   - Name match confidence checks.
   - Category blacklist and residential checks.
   - Delivery-required checks for selected business types.

5. **Merge and update**:
   - Existing Salesforce values are preserved.
   - Empty Salesforce fields are filled from enrichment data.
   - Bulk update is executed in batches.

## 📁 Project Structure

| File | Role |
|------|------|
| `lead_enrichment_automation.py` | **Orchestrator** — Salesforce query, threading, merge logic, bulk update |
| `logic.py` | **Engine** — Market handlers, FSA integration, qualification rules, address parsing |
| `qualification_config.json` | **Ruleset** — Human-editable JSON controlling all qualification behaviour |
| `market_config.json` | **Market metadata** — Country aliases/GL + competitor platform mapping |

## 🍽️ Cuisine Detection (Important)

Cuisine extraction is intentionally layered to reduce false positives:

1. **Google type parsing** (`extract_cuisines`):
   - Reads `type_ids` + `type` from SerpAPI.
   - Removes generic types (e.g. `restaurant`, `food`, `establishment`).
   - Cleans labels and normalizes both display names and snake_case IDs.

2. **Known cuisine matching**:
   - Uses canonical cuisine keywords from `qualification_config.json -> rules.cuisine_keywords`.
   - Falls back to built-in cuisine defaults if config is empty/missing.
   - Handles normalized variants so labels like `turkish_restaurant` and `Turkish Restaurant` map to `Turkish`.

3. **Priority buckets**:
   - **Confirmed cuisines** (best): explicit known cuisine labels.
   - **Other non-generic types**: e.g. family/informal categories.
   - **Meal/service types**: breakfast/brunch/fast-food style labels.
   - **Venue fallback**: cafe/bar/pub/bakery when no better cuisine is found.

4. **Menu/website fallback** (`fetch_menu_and_determine_cuisine`):
   - Triggered when primary cuisine is missing or not recognized.
   - Scrapes menu link and/or website text.
   - Removes script/style/HTML noise before keyword matching.

**Output fields:**
- `Primary_Cuisine__c` = best inferred cuisine
- `Secondary_Cuisine__c` = second-best candidate
- `Cuisine_Type__c` maps to primary cuisine for compatibility

## 🇬🇧 UK FSA Enrichment (Important)

For UK leads, `UKMarketHandler` enriches:

- `FSA_AGENCY__c`
- `FSA_RATING__c`
- `FSA_URL__c`

Implementation details:

- Calls `https://api.ratings.food.gov.uk/Establishments` with API version header.
- Uses retry/backoff on rate limits (`429`) and request errors.
- Caches responses per `(normalized_name, postcode)` to avoid duplicate calls in threaded runs.
- Uses fuzzy matching against `BusinessName` and applies a minimum confidence threshold before writing data.
- Maps numeric FSA ratings (`0-5`) to Salesforce picklist values (`ZERO` ... `FIVE`).

## 🔗 Competitor URL Enrichment

Competitor links are added in market post-processing via `search_competitor_links`:

- Uses DuckDuckGo `site:` queries (no API key required).
- Uses a semaphore to reduce rate-limit pressure under concurrent processing.
- Uses fuzzy title validation before accepting a URL.
- Country-aware platform mapping:
  - UK/BE: Uber Eats + Deliveroo
  - NL/DE/AT/CH/US: Uber Eats only
- URLs are only written to Salesforce when found (existing manually entered values are not cleared).

Salesforce fields expected:

- `Uber_Eats_URL__c`
- `Deliveroo_URL__c`

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
| `Cuisine_Type__c` | Text | Primary cuisine (legacy compatibility field) |
| `Primary_Cuisine__c` | Text | Top cuisine |
| `Secondary_Cuisine__c` | Text | Second cuisine |
| `Opening_Hours__c` | Long Text Area (32 768) | Compact formatted hours |
| `Phone` (standard) | Phone | Stored in E.164 format when normalization succeeds |
| `FSA_AGENCY__c` | Text | UK only — local authority name |
| `FSA_RATING__c` | Picklist | UK only — `ZERO` … `FIVE` |
| `FSA_URL__c` | URL | UK only — FSA business page |
| `Uber_Eats_URL__c` | URL | Competitor listing URL when detected |
| `Deliveroo_URL__c` | URL | Competitor listing URL when detected (market-dependent) |
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
    uv run python lead_enrichment_automation.py
    ```

Optional quick sanity check:

```bash
uv run python -m compileall logic.py lead_enrichment_automation.py
```

The script queries leads where `Google_Place_ID__c != NULL` and either qualification is empty or phone is missing, then applies a Python-side filter to skip records that already have enrichment fields populated.

## 🧠 Merge & Data Safety Notes

- Existing Salesforce values are preserved by default.
- Only empty Salesforce fields are backfilled from enrichment output (numeric `0` is treated as a real value, not as missing).
- Text fields are sanitized (control characters removed, trimmed to field-safe lengths).
- Phone numbers from Google Maps are normalized to E.164 (`+CCXXXXXXXXX`) using the lead's `Country` to infer the region. If parsing fails, the original (sanitized) value is kept.
- Competitor URLs are only written when a non-empty URL is found.
- `Opening_Hours__c` is treated as Long Text Area and supports up to 32,768 chars.
- The standard `Status` field is **not** written by the automation. Qualification state is tracked exclusively on `Qualification_Status__c`, so existing sales workflow values (e.g. `Working - Contacted`) are never overwritten on re-enrichment.

## ⚙️ Configuration (`qualification_config.json`)

All qualification behaviour is controlled by `rules` keys in `qualification_config.json`. No code changes are needed for day-to-day tuning.

| Key | Type | Effect |
|-----|------|--------|
| `require_delivery_for_types` | list[str] | Disqualify these place types unless delivery is enabled |
| `require_delivery_for_name_keywords` | list[str] | Same check triggered by business name keywords (whole-word match) |
| `always_disqualify_types` | list[str] | Unconditionally disqualify these place types |
| `residential_signals` | list[str] | Google address component types that indicate a residential address |
| `residential_exception_types` | list[str] | Override residential disqualification for these types |
| `pre_qualification_rules.disqualify_name_keywords` | list[str] | Free pre-filter on Salesforce name/company (whole-word match) |
| `pre_qualification_rules.disqualify_address_keywords` | list[str] | Free pre-filter on Salesforce street address (whole-word match) |
| `cuisine_keywords` | list[str] | Canonical cuisine vocabulary used for both Google-type matching and menu/website fallback detection |

> **Keyword matching note:** all `*_name_keywords` / `*_address_keywords` lists use **word-boundary** matching, so e.g. `"bar"` only matches the whole word `bar` and will not falsely match `Barbara's Pizzeria` or `Greatest Pizzas`.

## 🌐 Market Configuration (`market_config.json`)

Market routing and competitor scraping inputs are configured in `market_config.json`:

- `market_countries`: canonical market key (`UK`, `NL`, `DE`, ...) with:
  - `gl` (SerpAPI country code)
  - `aliases` (accepted lead-country variants)
- `competitor_platforms`: platform list per country key with:
  - `name`
  - `key` (enrichment payload key, e.g. `uber_eats_url`)
  - `site` (domain used for `site:` search)

The loader validates structure and falls back to built-in defaults if the file is missing or malformed.

## 🌍 Adding New Markets

For standard (non-UK) markets, adding a new country is config-only:

1. Add an entry under `market_countries` in `market_config.json`.
2. Add competitor platform definitions under `competitor_platforms`.

For UK-style custom enrichment (like FSA), add a dedicated handler in `logic.py` and route that canonical country key in `MarketFactory`.
