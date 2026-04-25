import os
import datetime
import logging
import pandas as pd
from typing import Dict, Any, Optional
from tqdm.auto import tqdm
from dotenv import load_dotenv
from serpapi import GoogleSearch
from simple_salesforce import Salesforce

# Import modularized logic
from logic import (
    extract_store_type,
    extract_service_options,
    extract_payment_options,
    extract_address_components,
    evaluate_qualification,
    pre_qualify_lead,
    MarketFactory
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

# --- Functions moved to logic.py (MarketHandler) ---

# Connect to Salesforce and Query Leads
try:
    sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)
    logger.info("Connected to Salesforce")
    query = """
    SELECT Id, Company, Name, Phone, Website, Street, City, PostalCode, Country, 
           Google_Place_ID__c, Store_Type__c, Price_Range__c, Google_Rating__c, 
           Service_Options__c, Payment_Options__c, Status, Qualification_Status__c,
           FSA_AGENCY__c, FSA_RATING__c, FSA_URL__c
    FROM Lead
    WHERE Google_Place_ID__c != NULL 
    AND Qualification_Status__c = NULL
    """
    leads = sf.query(query)['records']
    df = pd.DataFrame(leads).drop(columns=['attributes'], errors='ignore')
    logger.info(f"Retrieved {len(df)} leads to enrich")
except Exception as e:
    logger.error(f"Salesforce operation failed: {e}")
    raise

# Process Enrichment with Merge Logic
preview_rows = []
for _, row in tqdm(df.iterrows(), total=len(df), desc="Enriching Leads"):
    try:
        # 1. Pre-Qualification Check (FREE)
        is_pre_qualified, pre_reason = pre_qualify_lead(row)
        
        if not is_pre_qualified:
            logger.info(f"Lead {row['Name']} disqualified early (Saving Credits): {pre_reason}")
            merged = {
                "Id": row["Id"],
                "Qualification_Status__c": "Disqualified",
                "Disqualification_Reason__c": pre_reason[:255],
                "Status": "Disqualified",
                "Date_Enriched_At__c": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            preview_rows.append(merged)
            continue

        # 2. Enrichment (Handlers manage both SerpAPI and Market-specific data)
        handler = MarketFactory.get_handler(row.get("Country"), SERP_API)
        data = handler.enrich(row)
        if data:
            # Merge logic: Only fill fields that are empty in Salesforce
            merged = {"Id": row["Id"], "Name": row["Name"]}
            field_map = {
                "Phone": "phone", "Website": "website", "Store_Type__c": "store_type",
                "Price_Range__c": "price_range", "Google_Rating__c": "rating", 
                "Google_Reviews__c": "reviews", "Service_Options__c": "service_options",
                "Payment_Options__c": "payment_options", "Street": "street", 
                "City": "city", "PostalCode": "postal_code", "Country": "country",
                "FSA_AGENCY__c": "FSA_AGENCY", "FSA_RATING__c": "FSA_RATING", 
                "FSA_URL__c": "FSA_URL"
            }
            for sf_field, google_key in field_map.items():
                val = row.get(sf_field)
                if pd.isna(val) or not val:
                    merged[sf_field] = data.get(google_key)
                else:
                    merged[sf_field] = val # Keep original
            
            # Apply Qualification Logic with Strict Address Check
            status, reason = evaluate_qualification(
                sf_company=row['Company'],
                google_name=data.get("google_name"),
                types=data["raw_types"], 
                service_options=data["raw_service_options"],
                business_status=data.get("business_status"),
                permanently_closed=data.get("permanently_closed", False),
                sf_street=row.get("Street"),
                sf_postcode=row.get("PostalCode"),
                google_address=data.get("full_address")
            )
            
            # Update Salesforce Custom Fields
            merged["Qualification_Status__c"] = status
            if reason:
                merged["Disqualification_Reason__c"] = reason[:255]
                merged["Status"] = "Disqualified"
                logger.info(f"Lead {row['Name']} disqualified: {reason}")
            else:
                merged["Status"] = "Qualified"
                logger.info(f"Lead {row['Name']} qualified")

            merged.update({
                "Date_Enriched_At__c": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
            preview_rows.append(merged)
        else:
            logger.warning(f"Skipping {row['Name']} - No enrichment data found")
            
    except Exception as e:
        logger.error(f"Failed to process lead {row.get('Name')} ({row.get('Id')}): {e}")
        continue

# Update Salesforce
if not (preview_df := pd.DataFrame(preview_rows)).empty:
    # Clean data: Replace NaN with None (Salesforce doesn't accept NaN in JSON)
    records = preview_df.replace({pd.NA: None, float('nan'): None}).to_dict('records')
    
    for r in records: r.pop('Name', None) # Id is enough for update
    
    try:
        logger.info(f"Updating {len(records)} leads in Salesforce...")
        results = sf.bulk.Lead.update(records)
        successes = [r for r in results if r["success"]]
        failures = [r for r in results if not r["success"]]
        logger.info(f"Update complete: {len(successes)} successful, {len(failures)} failed")
        for f in failures:
            logger.error(f"Failed to update record: {f.get('errors')}")
    except Exception as e:
        logger.error(f"Bulk update failed: {e}")
else:
    logger.info("No data to update")