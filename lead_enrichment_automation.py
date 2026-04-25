import os
import re
import json
import datetime
import logging
import pandas as pd
from typing import Dict, Any, Optional, List
from tqdm.auto import tqdm
from dotenv import load_dotenv
from serpapi import GoogleSearch
from simple_salesforce import Salesforce

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
missing = [v for v in required if not os.getenv(v)]
if missing:
    raise ValueError(f"Missing environment variables: {', '.join(missing)}")
logger.info("Configuration loaded successfully")

# Load Qualification Rules from JSON
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'qualification_config.json')
try:
    with open(CONFIG_PATH, 'r') as f:
        QUAL_RULES = json.load(f).get('rules', {})
    logger.info("Qualification rules loaded successfully")
except Exception as e:
    logger.error(f"Failed to load qualification rules: {e}")
    QUAL_RULES = {}

# Utility Functions to parse Google Maps Data
def extract_store_type(place: Dict[str, Any]) -> Optional[str]:
    types = place.get("type", [])
    return ", ".join(types) if isinstance(types, list) else None

def extract_service_options(place: Dict[str, Any]) -> Optional[str]:
    options = place.get("service_options", {})
    return ", ".join([k for k, v in options.items() if v]) if isinstance(options, dict) else None

def extract_payment_options(place: Dict[str, Any]) -> Optional[str]:
    for item in place.get("extensions", []):
        if "payments" in item:
            return ", ".join(item["payments"])
    return None

def extract_address_components(place: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Parse address components from full address string with improved robustness."""
    full_address = place.get("address", "")
    res = {"street": None, "city": None, "postal_code": None, "country": None}
    if not full_address: return res
        
    parts = [p.strip() for p in full_address.split(",")]
    if len(parts) >= 1: res["street"] = parts[0]
    if len(parts) >= 2:
        res["country"] = parts[-1]
        city_postal_part = parts[-2] if len(parts) > 2 else parts[1]
        postal_match = re.search(r'\b\d{4,5}(?:[-\s][A-Z]{1,2})?\b', city_postal_part)
        if postal_match:
            res["postal_code"] = postal_match.group()
            res["city"] = city_postal_part.replace(res["postal_code"], "").strip().strip(',')
        else:
            res["city"] = city_postal_part
    return res

def evaluate_qualification(sf_name: str, google_name: Optional[str], types: List[str], service_options: Dict[str, Any], business_status: Optional[str] = None, permanently_closed: bool = False) -> (str, Optional[str]):
    """
    Qualification logic based on Restaurant standards defined in config.
    Returns (status, reason)
    """
    # Check both names for keywords to be more accurate
    combined_name = f"{sf_name} {google_name or ''}".lower()
    norm_types = [t.lower() for t in types]
    
    # 0. Check Business Operational Status (Closed/Out of Business)
    if permanently_closed or business_status == "CLOSED_PERMANENTLY":
        return "Disqualified", "Automation Disqualified: Location is marked as Permanently Closed on Google Maps."
    
    if business_status == "CLOSED_TEMPORARILY":
        return "Disqualified", "Automation Disqualified: Location is marked as Temporarily Closed on Google Maps."

    # 1. Always Disqualify Types (Schools, Hotels, etc.)
    blacklisted = QUAL_RULES.get('always_disqualify_types', [])
    for t in norm_types:
        if t in blacklisted:
            return "Disqualified", f"Automation Disqualified: Category '{t}' is on the exclusion list."

    # 2. Residential check
    res_signals = QUAL_RULES.get('residential_signals', [])
    res_exceptions = QUAL_RULES.get('residential_exception_types', [])
    is_residential = any(s in norm_types for s in res_signals) and not any(e in norm_types for e in res_exceptions)
    
    if is_residential:
        return "Disqualified", "Automation Disqualified: Residential Address (No public business listing found)."
    
    # 3. Bar/Cafe/Pub delivery requirement check
    req_types = QUAL_RULES.get('require_delivery_for_types', [])
    req_keywords = QUAL_RULES.get('require_delivery_for_name_keywords', [])
    
    # Check if business matches by type OR by name keyword
    is_target_business = any(t in norm_types for t in req_types) or any(k in combined_name for k in req_keywords)
    has_delivery = service_options.get("delivery", False)
    
    if is_target_business and not has_delivery:
        return "Disqualified", "Automation Disqualified: Establishment identified as Bar/Cafe/Pub without an active delivery service."
        
    return "Qualified", None

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
    if not df.empty: print(df.head()) # Not needed in Production
except Exception as e:
    logger.error(f"Salesforce operation failed: {e}")
    raise

# Process Enrichment with Merge Logic
preview_rows = []
for _, row in tqdm(df.iterrows(), total=len(df), desc="Enriching Leads"):
    try:
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
                # Ensure reason fits in 255 characters
                merged["Disqualification_Reason__c"] = reason[:255]
                merged["Status"] = "Disqualified" # Also update standard status for visibility
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
preview_df = pd.DataFrame(preview_rows)
logger.info(f"Successfully enriched {len(preview_df)} leads")
if not preview_df.empty: print(preview_df.head()) # Not needed in Production

# Update Salesforce
if not preview_df.empty:
    records = preview_df.to_dict('records')
    # Remove 'Name' if updating standard objects to avoid errors (Id is enough)
    for r in records: r.pop('Name', None)
    
    try:
        logger.info(f"Updating {len(records)} leads in Salesforce...")
        results = sf.bulk.Lead.update(records)
        successes = [r for r in results if r["success"]]
        failures = [r for r in results if not r["success"]]
        
        logger.info(f"Update complete: {len(successes)} successful, {len(failures)} failed")
        if failures:
            for i, f in enumerate(failures[:5]):
                lead_id = records[results.index(f)]['Id']
                errors = f.get('errors', [])
                logger.error(f"Lead {lead_id} failed: {errors[0].get('message') if errors else 'Unknown error'}")
    except Exception as e:
        logger.error(f"Bulk update failed: {e}")
else:
    logger.info("No data to update")