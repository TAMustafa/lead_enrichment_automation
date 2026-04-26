import os
import datetime
import logging
import pandas as pd
from tqdm.auto import tqdm
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from simple_salesforce import Salesforce

# Import modularized logic
from logic import (
    evaluate_qualification,
    pre_qualify_lead,
    sanitize_string,
    safe_isna,
    MarketFactory
)

# NOTE: Create these Salesforce custom fields before the first run:
#   Uber_Eats_URL__c (Type: URL, Length 255)
#   Deliveroo_URL__c (Type: URL, Length 255)
_COMPETITOR_FIELD_MAP: dict = {
    "Uber_Eats_URL__c": "uber_eats_url",
    "Deliveroo_URL__c": "deliveroo_url",
}

# SF text fields that must be sanitized (control-char stripped + truncated to 255)
_TEXT_FIELDS: set = {
    "Phone", "Website", "Store_Type__c", "Service_Options__c",
    "Payment_Options__c", "Street", "City", "PostalCode", "Country",
    "FSA_AGENCY__c", "FSA_URL__c",
    "Cuisine_Type__c", "Primary_Cuisine__c", "Secondary_Cuisine__c",
}

# Maps Salesforce field name → enriched data key
_FIELD_MAP: dict = {
    "Phone": "phone", "Website": "website", "Store_Type__c": "store_type",
    "Price_Range__c": "price_range", "Google_Rating__c": "rating",
    "Google_Reviews__c": "reviews", "Service_Options__c": "service_options",
    "Payment_Options__c": "payment_options", "Street": "street",
    "City": "city", "PostalCode": "postal_code", "Country": "country",
    "FSA_AGENCY__c": "FSA_AGENCY", "FSA_RATING__c": "FSA_RATING",
    "FSA_URL__c": "FSA_URL",
    "Cuisine_Type__c": "primary_cuisine",
    "Primary_Cuisine__c": "primary_cuisine",
    "Secondary_Cuisine__c": "secondary_cuisine",
    "Opening_Hours__c": "opening_hours",
}

_REQUIRED_ENRICHMENT_FIELDS: tuple = (
    "Qualification_Status__c",
    "Cuisine_Type__c",
    "Opening_Hours__c",
    "Phone",
)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(override=True)

# Configuration & Validation
SF_USERNAME = os.getenv('SF_USERNAME')
SF_PASSWORD = os.getenv('SF_PASSWORD')
SF_TOKEN = os.getenv('SF_TOKEN')
SERP_API = os.getenv('SERP_API')

required = ['SF_USERNAME', 'SF_PASSWORD', 'SF_TOKEN', 'SERP_API']
if missing := [v for v in required if not os.getenv(v)]:
    raise ValueError(f"Missing environment variables: {', '.join(missing)}")
logger.info("Configuration loaded successfully")

# Connect to Salesforce and Query Leads
try:
    sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)
    logger.info("Connected to Salesforce")
    # query_all automatically handles SOQL pagination (multiple REST pages),
    # so very large record sets are retrieved correctly without manual offset logic.
    query = """
    SELECT Id, Company, Name, Phone, Website, Street, City, PostalCode, Country,
           Google_Place_ID__c, Store_Type__c, Price_Range__c, Google_Rating__c,
           Google_Reviews__c, Service_Options__c, Payment_Options__c, Status, Qualification_Status__c,
           FSA_AGENCY__c, FSA_RATING__c, FSA_URL__c,
           Cuisine_Type__c, Primary_Cuisine__c, Secondary_Cuisine__c, Opening_Hours__c
    FROM Lead
    WHERE (Google_Place_ID__c != NULL 
    AND Qualification_Status__c = NULL)
    OR (Phone = NULL AND Google_Place_ID__c != NULL)
    """
    leads = sf.query_all(query)['records']
    df = pd.DataFrame(leads).drop(columns=['attributes'], errors='ignore')
    logger.info(f"Retrieved {len(df)} leads from Salesforce")
except Exception as e:
    logger.error(f"Salesforce operation failed: {e}")
    raise

# Process Enrichment with Merge Logic
preview_rows = []


def _is_missing(value) -> bool:
    """Return True when value is empty/NA and should be backfilled."""
    return safe_isna(value) or not value

def process_lead(row):
    try:
        # 1. Pre-Qualification Check (FREE)
        is_pre_qualified, pre_reason = pre_qualify_lead(row)
        
        if not is_pre_qualified:
            logger.info(f"Lead {row.get('Name')} disqualified early (Saving Credits): {pre_reason}")
            merged = {
                "Id": row["Id"],
                "Qualification_Status__c": "Disqualified",
                "Disqualification_Reason__c": sanitize_string(pre_reason),
                "Status": "Disqualified",
                "Date_Enriched_At__c": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            return merged

        # 2. Enrichment (Handlers manage both SerpAPI and Market-specific data)
        handler = MarketFactory.get_handler(row.get("Country"), SERP_API)
        data = handler.enrich(row)
        if data:
            # Merge logic: Only fill fields that are empty in Salesforce
            merged = {"Id": row["Id"]}
            for sf_field, google_key in _FIELD_MAP.items():
                val = row.get(sf_field)
                if _is_missing(val):
                    raw_val = data.get(google_key)
                    merged[sf_field] = sanitize_string(raw_val) if sf_field in _TEXT_FIELDS else raw_val
                else:
                    merged[sf_field] = val  # Keep original
            
            # Apply Qualification Logic with Strict Address Check
            status, reason = evaluate_qualification(
                sf_company=row.get('Company', ''),
                google_name=data.get("google_name"),
                types=data["raw_types"], 
                service_options=data["raw_service_options"],
                business_status=data.get("business_status"),
                permanently_closed=data.get("permanently_closed", False),
                sf_street=row.get("Street"),
                sf_postcode=row.get("PostalCode"),
                google_address=data.get("full_address"),
                address_component_types=data.get("raw_address_component_types"),
            )
            
            # Update Salesforce Custom Fields
            merged["Qualification_Status__c"] = status
            if reason:
                merged["Disqualification_Reason__c"] = sanitize_string(reason)
                merged["Status"] = "Disqualified"
                logger.info(f"Lead {row.get('Name')} disqualified: {reason}")
            else:
                merged["Disqualification_Reason__c"] = None
                merged["Status"] = "Qualified"
                logger.info(f"Lead {row.get('Name')} qualified")

            # Competitor links: only written when a URL is actually found — never sends
            # None to Salesforce, so manually-set values are never accidentally cleared.
            for sf_field, data_key in _COMPETITOR_FIELD_MAP.items():
                url = data.get(data_key)
                if url:
                    merged[sf_field] = sanitize_string(url)

            merged.update({
                "Date_Enriched_At__c": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            return merged
        else:
            logger.warning(f"Skipping {row.get('Name')} - No enrichment data found")
            return None
            
    except Exception as e:
        logger.exception(f"Failed to process lead {row.get('Name')} ({row.get('Id')}): {e}")
        return None

# Filter leads in Python since Salesforce SOQL can't filter Long Text Area fields
records_to_process = []
for row in df.to_dict('records'):
    if any(_is_missing(row.get(field)) for field in _REQUIRED_ENRICHMENT_FIELDS):
        # Skip if already disqualified (no need to waste credits fetching hours)
        if row.get('Qualification_Status__c') == 'Disqualified':
            continue
        records_to_process.append(row)

logger.info(f"Filtering complete: {len(records_to_process)} leads actually need enrichment.")

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = {executor.submit(process_lead, row): row for row in records_to_process}
    for future in tqdm(as_completed(futures), total=len(records_to_process), desc="Enriching Leads"):
        res = future.result()
        if res:
            preview_rows.append(res)

# Update Salesforce in Batches
if not (preview_df := pd.DataFrame(preview_rows)).empty:
    # Clean data: Replace NaN with None (Salesforce doesn't accept NaN in JSON)
    records = preview_df.replace({pd.NA: None, float('nan'): None}).to_dict('records')

    try:
        logger.info(f"Updating {len(records)} leads in Salesforce...")
        BATCH_SIZE = 2000
        all_successes = []
        all_failures = []
        
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]
            logger.info(f"Processing batch {i//BATCH_SIZE + 1} of {(len(records) + BATCH_SIZE - 1)//BATCH_SIZE}...")
            results = sf.bulk.Lead.update(batch)
            
            all_successes.extend([r for r in results if r["success"]])
            failures = [r for r in results if not r["success"]]
            all_failures.extend(failures)
            
            for f in failures:
                logger.error(f"Failed to update record: {f.get('errors')}")
                
        logger.info(f"Update complete: {len(all_successes)} successful, {len(all_failures)} failed")
    except Exception as e:
        logger.error(f"Bulk update failed: {e}")
else:
    logger.info("No data to update")