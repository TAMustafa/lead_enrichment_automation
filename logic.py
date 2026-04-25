import os
import re
import json
import logging
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple

logger = logging.getLogger(__name__)

# Load Qualification Rules
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'qualification_config.json')
try:
    with open(CONFIG_PATH, 'r') as f:
        QUAL_RULES = json.load(f).get('rules', {})
except Exception as e:
    logger.error(f"Failed to load qualification rules: {e}")
    QUAL_RULES = {}

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
    """Parse address components from full address string."""
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

def evaluate_qualification(sf_company: str, google_name: Optional[str], types: List[str], service_options: Dict[str, Any], business_status: Optional[str] = None, permanently_closed: bool = False) -> Tuple[str, Optional[str]]:
    """Qualification logic based on Restaurant standards defined in config."""
    combined_name = f"{sf_company} {google_name or ''}".lower()
    norm_types = [t.lower() for t in types]
    
    if permanently_closed or business_status == "CLOSED_PERMANENTLY":
        return "Disqualified", "Automation Disqualified: Location is marked as Permanently Closed on Google Maps."
    
    if business_status == "CLOSED_TEMPORARILY":
        return "Disqualified", "Automation Disqualified: Location is marked as Temporarily Closed on Google Maps."

    if "establishment" not in norm_types and any(t in norm_types for t in ["premise", "street_address", "route"]):
        return "Disqualified", "Automation Disqualified: Result is a generic address point, not a business listing."

    if google_name:
        s_comp, g_name = sf_company.lower().strip(), google_name.lower().strip()
        if s_comp not in g_name and g_name not in s_comp and "establishment" not in norm_types:
            return "Disqualified", f"Automation Disqualified: Name mismatch (SF Company: '{sf_company}', Google: '{google_name}')."

    blacklisted = QUAL_RULES.get('always_disqualify_types', [])
    for t in norm_types:
        if t in blacklisted:
            return "Disqualified", f"Automation Disqualified: Category '{t}' is blacklisted."

    res_signals = QUAL_RULES.get('residential_signals', [])
    res_exceptions = QUAL_RULES.get('residential_exception_types', [])
    if any(s in norm_types for s in res_signals) and not any(e in norm_types for e in res_exceptions):
        return "Disqualified", "Automation Disqualified: Residential Address detected."
    
    req_types = QUAL_RULES.get('require_delivery_for_types', [])
    req_keywords = QUAL_RULES.get('require_delivery_for_name_keywords', [])
    is_target = any(t in norm_types for t in req_types) or any(k in combined_name for k in req_keywords)
    
    if is_target and not service_options.get("delivery", False):
        return "Disqualified", "Automation Disqualified: Bar/Cafe/Pub without delivery service."
        
    return "Qualified", None

def pre_qualify_lead(row: pd.Series) -> Tuple[bool, Optional[str]]:
    """Perform 'free' checks on Salesforce data before spending API credits."""
    rules = QUAL_RULES.get('pre_qualification_rules', {})
    combined_name = f"{row.get('Name', '')} {row.get('Company', '')}".lower()
    for kw in rules.get('disqualify_name_keywords', []):
        if kw in combined_name:
            return False, f"Pre-Enrichment Disqualified: Junk keyword '{kw}' detected."
            
    street = str(row.get('Street', '')).lower()
    for kw in rules.get('disqualify_address_keywords', []):
        if re.search(rf'\b{kw}\b', street):
            return False, f"Pre-Enrichment Disqualified: Residential indicator '{kw}' detected."
            
    return True, None
