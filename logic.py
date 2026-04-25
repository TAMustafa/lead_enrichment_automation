import os
import re
import json
import logging
import requests
import unicodedata
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timezone
from rapidfuzz import fuzz
from serpapi import GoogleSearch

logger = logging.getLogger(__name__)

# Pre-compiled Regex for Performance
POSTAL_REGEX = re.compile(r'([A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}|\b\d{4,5}(?:[-\s][A-Z]{1,2})?\b)', re.I)

# Constants
GENERIC_CUISINE_TYPES = {'restaurant', 'food', 'point_of_interest', 'establishment', 'store', 'meal_takeaway', 'meal_delivery'}

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

def extract_opening_hours(place: Dict[str, Any]) -> Optional[str]:
    hours = place.get("hours")
    
    # Handle alternative SerpApi formats just in case
    if not hours:
        alt_hours = place.get("operating_hours")
        if isinstance(alt_hours, dict):
            hours = [{k: v} for k, v in alt_hours.items()]
            
    if not isinstance(hours, list):
        return None
        
    formatted = []
    for day_entry in hours:
        if isinstance(day_entry, dict):
            for day, time_str in day_entry.items():
                # Shorten day names to save space (e.g., 'monday' -> 'Mon')
                day_short = day[:3].capitalize()
                formatted.append(f"{day_short}: {time_str}")
        
    result = " | ".join(formatted)
    return result[:255] if result else None

def extract_cuisines(place: Dict[str, Any]) -> Dict[str, Optional[str]]:
    raw_types = place.get("type", [])
    if not isinstance(raw_types, list):
        raw_types = []
        
    cuisines = []
    for t in raw_types:
        t_lower = t.lower()
        if t_lower not in GENERIC_CUISINE_TYPES:
            # Clean up the string
            cleaned = t_lower.replace('_restaurant', '').replace(' restaurant', '')
            cleaned = cleaned.replace('_takeaway', '').replace(' takeaway', '')
            cleaned = cleaned.replace('_', ' ').title().strip()
            if cleaned and cleaned not in cuisines:
                cuisines.append(cleaned)
                
    # If we still have nothing, maybe check if it's a cafe, bar, bakery
    if not cuisines:
        for t in raw_types:
            t_lower = t.lower()
            if t_lower in {'cafe', 'bar', 'pub', 'bakery', 'coffee_shop', 'fast_food'}:
                cleaned = t_lower.replace('_', ' ').title().strip()
                if cleaned not in cuisines:
                    cuisines.append(cleaned)
                    
    cuisine_type = ", ".join(cuisines) if cuisines else None
    primary_cuisine = cuisines[0] if len(cuisines) > 0 else None
    secondary_cuisine = cuisines[1] if len(cuisines) > 1 else None

    # Truncate to avoid Salesforce STRING_TOO_LONG errors (assuming 15 char limit)
    if cuisine_type and len(cuisine_type) > 15:
        # If it's a list, try to just use primary
        cuisine_type = primary_cuisine[:15] if primary_cuisine else cuisine_type[:15]
    if primary_cuisine and len(primary_cuisine) > 15:
        primary_cuisine = primary_cuisine[:15]
    if secondary_cuisine and len(secondary_cuisine) > 15:
        secondary_cuisine = secondary_cuisine[:15]

    return {
        "cuisine_type": cuisine_type,
        "primary_cuisine": primary_cuisine,
        "secondary_cuisine": secondary_cuisine
    }

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
        
        # Improved Postcode Regex (Supports UK: YO10 4AH, US: 12345, EU: 1234)
        postal_match = POSTAL_REGEX.search(city_postal_part)
        if postal_match:
            res["postal_code"] = postal_match.group()
            res["city"] = city_postal_part.replace(res["postal_code"], "").strip().strip(',')
        else:
            res["city"] = city_postal_part
    return res

def normalize_string(s: str) -> str:
    """Remove accents and special characters for better matching."""
    if not s: return ""
    s = s.lower().strip()
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return re.sub(r'[^a-z0-9\s]', '', s)

def evaluate_qualification(
    sf_company: str, 
    google_name: Optional[str], 
    types: List[str], 
    service_options: Dict[str, Any], 
    business_status: Optional[str] = None, 
    permanently_closed: bool = False,
    sf_street: Optional[str] = None,
    sf_postcode: Optional[str] = None,
    google_address: Optional[str] = None
) -> Tuple[str, Optional[str]]:
    """Qualification logic with strict Name and Address verification."""
    norm_sf_name = normalize_string(sf_company)
    norm_google_name = normalize_string(google_name)
    norm_types = [t.lower() for t in types]
    
    # 1. Closed Status Check
    if permanently_closed or business_status == "CLOSED_PERMANENTLY":
        return "Disqualified", "Automation Disqualified: Location is marked as Permanently Closed."
    
    if business_status == "CLOSED_TEMPORARILY":
        return "Disqualified", "Automation Disqualified: Location is marked as Temporarily Closed."

    # 2. Address Verification (The "Ground Truth" Check)
    if google_address and (sf_street or sf_postcode):
        norm_google_addr = normalize_string(google_address)
        norm_sf_street = normalize_string(sf_street)
        norm_sf_postcode = normalize_string(sf_postcode)
        
        # Check if either street or postcode is found in the Google address
        street_match = norm_sf_street and norm_sf_street in norm_google_addr
        postcode_match = norm_sf_postcode and norm_sf_postcode in norm_google_addr
        
        if not street_match and not postcode_match:
            return "Disqualified", f"Automation Disqualified: Location Mismatch (SF Address: '{sf_street} {sf_postcode}', Google: '{google_address}')."

    # 3. Name Match Check
    if norm_google_name and norm_sf_name:
        match_score = fuzz.token_set_ratio(norm_sf_name, norm_google_name)
        if match_score < 75:
            # Exception: if it's a generic establishment type, we might be more lenient, 
            # but usually a name mismatch is a dealbreaker.
            if "establishment" not in norm_types:
                return "Disqualified", f"Automation Disqualified: Name mismatch (SF: '{sf_company}', Google: '{google_name}', Score: {match_score})."

    # 4. Category Blacklist
    blacklisted = QUAL_RULES.get('always_disqualify_types', [])
    for t in norm_types:
        if t in blacklisted:
            return "Disqualified", f"Automation Disqualified: Category '{t}' is blacklisted."

    # 5. Residential Check
    res_signals = QUAL_RULES.get('residential_signals', [])
    res_exceptions = QUAL_RULES.get('residential_exception_types', [])
    if any(s in norm_types for s in res_signals) and not any(e in norm_types for e in res_exceptions):
        return "Disqualified", "Automation Disqualified: Residential Address detected."
    
    # 6. Service Delivery Check
    req_types = QUAL_RULES.get('require_delivery_for_types', [])
    req_keywords = QUAL_RULES.get('require_delivery_for_name_keywords', [])
    combined_name = f"{sf_company} {google_name or ''}".lower()
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

# Market Specific Handlers
class MarketHandler:
    """Base class for enrichment. Handles standard Google Maps enrichment."""
    def __init__(self, country_code: str, api_key: Optional[str] = None):
        self.country_code = country_code
        self.api_key = api_key
        
    def _get_gl(self) -> str:
        """Map country variations to 2-letter GL code for SerpAPI."""
        mapping = {
            "UK": "uk", "GB": "uk", "UNITED KINGDOM": "uk", "GREAT BRITAIN": "uk",
            "DE": "de", "GERMANY": "de", "NL": "nl", "NETHERLANDS": "nl",
            "BE": "be", "BELGIUM": "be", "AT": "at", "AUSTRIA": "at",
            "CH": "ch", "SWITZERLAND": "ch", "US": "us", "UNITED STATES": "us"
        }
        return mapping.get(self.country_code.upper(), "us")

    def enrich(self, lead_row: pd.Series) -> Optional[Dict[str, Any]]:
        """Enrichment using SerpAPI with fallback logic."""
        place_id = lead_row.get("Google_Place_ID__c")
        name = lead_row.get("Company") or lead_row.get("Name")
        city = lead_row.get("City")
        postcode = lead_row.get("PostalCode")
        country = lead_row.get("Country")
        
        if not place_id and not name: return None
            
        params = {"engine": "google_maps", "api_key": self.api_key, "gl": self._get_gl()}
        
        # Strategy 1: Try Place ID
        if place_id:
            params["place_id"] = place_id
            try:
                results = GoogleSearch(params).get_dict()
                if place := results.get("place_results"):
                    return self._process_results(place, lead_row)
            except Exception as e:
                logger.warning(f"Place ID search failed for {place_id}: {e}")

        # Strategy 2: Fallback to Name + Detailed Location search
        # Combining all address details for a "Unique Signature" search
        addr_parts = [p for p in [lead_row.get("Street"), lead_row.get("City"), lead_row.get("PostalCode")] if p]
        loc_context = ", ".join(addr_parts) or country or ""
        search_query = f"{name} {loc_context}".strip()
        
        params.pop("place_id", None)
        params["q"] = search_query
        try:
            results = GoogleSearch(params).get_dict()
            if place := results.get("place_results"):
                return self._process_results(place, lead_row)
            elif local := results.get("local_results"):
                # Filter local results for name match if possible
                for r in local:
                    if normalize_string(name) in normalize_string(r.get("title")):
                        return self._process_results(r, lead_row)
                return self._process_results(local[0], lead_row)
        except Exception as e:
            logger.error(f"Fallback search failed for {search_query}: {e}")
            
        return None

    def _process_results(self, place: Dict[str, Any], lead_row: pd.Series) -> Dict[str, Any]:
        """Common logic to parse SerpAPI results."""
        addr = extract_address_components(place)
        cuisines = extract_cuisines(place)
        data = {
            "google_name": place.get("title"),
            "phone": place.get("phone"),
            "website": place.get("website"),
            "rating": place.get("rating"),
            "reviews": place.get("reviews"),
            "price_range": place.get("price"),
            "store_type": extract_store_type(place),
            "service_options": extract_service_options(place),
            "payment_options": extract_payment_options(place),
            "cuisine_type": cuisines["cuisine_type"],
            "primary_cuisine": cuisines["primary_cuisine"],
            "secondary_cuisine": cuisines["secondary_cuisine"],
            "opening_hours": extract_opening_hours(place),
            **addr,
            "full_address": place.get("address"),
            "raw_types": place.get("type", []),
            "raw_service_options": place.get("service_options", {}),
            "business_status": place.get("business_status"),
            "permanently_closed": place.get("permanently_closed", False)
        }
        return self.post_enrich(data, lead_row)

    def post_enrich(self, data: Dict[str, Any], lead_row: pd.Series) -> Dict[str, Any]:
        """Hook for market-specific extra steps."""
        return data

class UKMarketHandler(MarketHandler):
    """UK-specific enrichment adding FSA Hygiene data."""
    FSA_API_BASE = "https://api.ratings.food.gov.uk"
    
    def post_enrich(self, data: Dict[str, Any], lead_row: pd.Series) -> Dict[str, Any]:
        raw_name = data.get("google_name") or lead_row.get("Company")
        postcode = data.get("postal_code") or lead_row.get("PostalCode")
        
        if not raw_name: return data
        
        # FSA API is picky with accents - normalize for better matching
        search_name = normalize_string(raw_name)
            
        try:
            headers = {"x-api-version": "2", "Accept": "application/json"}
            params = {"name": search_name, "address": postcode} if postcode else {"name": search_name}
            res = requests.get(f"{self.FSA_API_BASE}/Establishments", params=params, headers=headers, timeout=5)
            
            if res.status_code == 200:
                establishments = res.json().get("establishments", [])
                if establishments:
                    # Look for the best match in results
                    match = establishments[0]
                    for e in establishments:
                        if fuzz.token_set_ratio(normalize_string(raw_name), normalize_string(e.get("BusinessName"))) >= 80:
                            match = e
                            break
                            
                    # Map numeric ratings to your Salesforce picklist words
                    rating_map = {
                        "0": "ZERO", "1": "ONE", "2": "TWO", 
                        "3": "THREE", "4": "FOUR", "5": "FIVE"
                    }
                    raw_rating = str(match.get("RatingValue"))
                    
                    data.update({
                        "FSA_AGENCY": match.get("LocalAuthorityName"),
                        "FSA_RATING": rating_map.get(raw_rating, raw_rating),
                        "FSA_URL": f"https://ratings.food.gov.uk/business/en-GB/{match.get('FHRSID')}"
                    })
                    logger.info(f"FSA data found for {raw_name}")
                else:
                    logger.info(f"No FSA data found for {search_name} in {postcode}")
        except requests.exceptions.RequestException as e:
            logger.error(f"FSA API request error: {e}")
        except Exception as e:
            logger.error(f"FSA parsing error: {e}")
            
        return data

class MarketFactory:
    """Factory to return the appropriate MarketHandler."""
    
    # Map multiple aliases to the same handler class
    _MAPPING = {
        UKMarketHandler: ["UK", "GB", "UNITED KINGDOM", "GREAT BRITAIN"],
        MarketHandler: ["NL", "NETHERLANDS", "DE", "GERMANY", "AT", "AUSTRIA", "CH", "SWITZERLAND", "BE", "BELGIUM", "US"]
    }
    
    @classmethod
    def get_handler(cls, country: Optional[str], api_key: str) -> MarketHandler:
        country_norm = str(country or "US").upper().strip()
        
        for handler_class, aliases in cls._MAPPING.items():
            if country_norm in aliases:
                return handler_class(country_norm, api_key)
        
        return MarketHandler("DEFAULT", api_key)
