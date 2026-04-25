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
    pre_qualify_lead
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

def enrich_with_serpapi(place_id: Optional[str] = None, name: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Enrich lead data using SerpAPI Google Maps search."""
    if not place_id and not name: raise ValueError("Either place_id or name must be provided")
    params = {"engine": "google_maps", "api_key": SERP_API}
    if place_id: params["place_id"] = place_id
    else: params["q"] = name
    
    try:
        results = GoogleSearch(params).get_dict()
        place = results.get("place_results", {})
        if not place:
            logger.warning(f"No results for {place_id or name}")
            return None
            
        addr = extract_address_components(place)
        return {
            "google_name": place.get("title"),
            "phone": place.get("phone"),
            "website": place.get("website"),
            "rating": place.get("rating"),
            "reviews": place.get("reviews"),
            "price_range": place.get("price"),
            "store_type": extract_store_type(place),
            "service_options": extract_service_options(place),
            "payment_options": extract_payment_options(place),
            **addr,
            "full_address": place.get("address"),
            "raw_types": place.get("type", []),
            "raw_service_options": place.get("service_options", {}),
            "business_status": place.get("business_status"),
            "permanently_closed": place.get("permanently_closed", False)
        }
    except Exception as e:
        logger.error(f"Error enriching {place_id or name}: {e}")
        return None

# Connect to Salesforce and Query Leads
try:
    sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_TOKEN)
    logger.info("Connected to Salesforce")
    query = """
    SELECT Id, Company, Name, Phone, Website, Street, City, PostalCode, Country, 
           Google_Place_ID__c, Store_Type__c, Price_Range__c, Google_Rating__c, 
           Service_Options__c, Payment_Options__c, Status, Qualification_Status__c
    FROM Lead
    WHERE Google_Place_ID__c != NULL 
    AND Qualification_Status__c = NULL
    LIMIT 10
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

        # 2. Enrichment (PAID - Costs 1 Credit)
        data = enrich_with_serpapi(place_id=row.get("Google_Place_ID__c"), name=row.get("Name"))
        if data:
            # Merge logic: Only fill fields that are empty in Salesforce
            merged = {"Id": row["Id"], "Name": row["Name"]}
            field_map = {
                "Phone": "phone", "Website": "website", "Store_Type__c": "store_type",
                "Price_Range__c": "price_range", "Google_Rating__c": "rating", 
                "Google_Reviews__c": "reviews", "Service_Options__c": "service_options",
                "Payment_Options__c": "payment_options", "Street": "street", 
                "City": "city", "PostalCode": "postal_code", "Country": "country"
            }
            for sf_field, google_key in field_map.items():
                val = row.get(sf_field)
                if pd.isna(val) or not val:
                    merged[sf_field] = data.get(google_key)
                else:
                    merged[sf_field] = val # Keep original
            
            # Apply Qualification Logic
            status, reason = evaluate_qualification(
                sf_name=row['Name'],
                google_name=data.get("google_name"),
                types=data["raw_types"], 
                service_options=data["raw_service_options"],
                business_status=data.get("business_status"),
                permanently_closed=data.get("permanently_closed", False)
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
    records = preview_df.to_dict('records')
    for r in records: r.pop('Name', None) # Id is enough for update
    
    try:
        logger.info(f"Updating {len(records)} leads in Salesforce...")
        results = sf.bulk.Lead.update(records)
        successes = [r for r in results if r["success"]]
        failures = [r for r in results if not r["success"]]
        logger.info(f"Update complete: {len(successes)} successful, {len(failures)} failed")
    except Exception as e:
        logger.error(f"Bulk update failed: {e}")
else:
    logger.info("No data to update")