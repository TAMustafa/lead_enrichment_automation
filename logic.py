import os
import re
import time
import json
import logging
import threading
from copy import deepcopy
import unicodedata
import requests
import pandas as pd
from typing import Dict, Any, Optional, List, Tuple
from rapidfuzz import fuzz
from serpapi import GoogleSearch

logger = logging.getLogger(__name__)

# --- Optional: DuckDuckGo free search for competitor links ---
try:
    from ddgs import DDGS as _DDGS
    _DDG_AVAILABLE = True
except ImportError:
    _DDGS = None  # type: ignore[assignment]
    _DDG_AVAILABLE = False
    logger.warning("ddgs not installed — competitor link search disabled. Run: uv add ddgs")

# --- Shared HTTP session for connection pooling ---
# Reusing a single Session across all requests avoids the overhead of
# creating a new TCP connection per call (relevant for FSA + menu scraping).
_http_session = requests.Session()
_http_session.headers.update({"User-Agent": "Mozilla/5.0"})

# --- FSA API base URL (single place to update if the URL ever changes) ---
FSA_API_BASE = "https://api.ratings.food.gov.uk"

# --- DuckDuckGo rate-limit guard ---
# Cap concurrent DDG calls to 3 even when the main executor runs 10 threads.
_ddg_semaphore = threading.Semaphore(3)
_DDG_SLEEP: float = 0.4  # seconds to pause after acquiring the semaphore

# --- Competitor platforms per country (Just Eat owns Thuisbezorgd & Lieferando — excluded) ---
# Keys in the returned dict map directly to Salesforce custom field names (without __c suffix).
# NOTE: You must create these custom fields in Salesforce before the first run:
#   Uber_Eats_URL__c  (URL, length 255)
#   Deliveroo_URL__c  (URL, length 255)
_DEFAULT_MARKET_COUNTRIES: Dict[str, Dict[str, List[str] | str]] = {
    "UK": {
        "gl": "uk",
        "aliases": ["UK", "GB", "UNITED KINGDOM", "GREAT BRITAIN"],
    },
    "NL": {
        "gl": "nl",
        "aliases": ["NL", "NETHERLANDS"],
    },
    "DE": {
        "gl": "de",
        "aliases": ["DE", "GERMANY"],
    },
    "AT": {
        "gl": "at",
        "aliases": ["AT", "AUSTRIA"],
    },
    "CH": {
        "gl": "ch",
        "aliases": ["CH", "SWITZERLAND"],
    },
    "BE": {
        "gl": "be",
        "aliases": ["BE", "BELGIUM"],
    },
    "US": {
        "gl": "us",
        "aliases": ["US", "UNITED STATES"],
    },
}


def _build_country_lookup_maps(
    market_countries: Dict[str, Dict[str, List[str] | str]],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Build alias and GL maps from canonical market country metadata."""
    aliases: Dict[str, str] = {}
    gl_map: Dict[str, str] = {}

    for canonical, meta in market_countries.items():
        canonical_norm = str(canonical).upper().strip()
        gl_value = str(meta.get("gl", "us")).lower().strip()
        if canonical_norm:
            gl_map[canonical_norm] = gl_value or "us"

        raw_aliases = meta.get("aliases", [])
        if isinstance(raw_aliases, list):
            for alias in raw_aliases:
                alias_norm = str(alias).upper().strip()
                if alias_norm:
                    aliases[alias_norm] = canonical_norm

        if canonical_norm:
            aliases[canonical_norm] = canonical_norm

    if "US" not in gl_map:
        gl_map["US"] = "us"
    aliases.setdefault("US", "US")
    return aliases, gl_map


def _is_valid_market_country_entry(entry: Any) -> bool:
    """Return True when one market country row has required keys/types."""
    if not isinstance(entry, dict):
        return False
    gl = entry.get("gl")
    aliases = entry.get("aliases")
    if not isinstance(gl, str) or not gl.strip():
        return False
    if not isinstance(aliases, list) or not aliases:
        return False
    return all(isinstance(a, str) and a.strip() for a in aliases)


def _is_valid_competitor_platform_entry(entry: Any) -> bool:
    """Return True when one competitor platform row has required keys/types."""
    if not isinstance(entry, dict):
        return False
    required_keys = ("name", "key", "site")
    return all(isinstance(entry.get(k), str) and entry.get(k).strip() for k in required_keys)


def _load_market_config(
    path: str,
) -> Tuple[Dict[str, Dict[str, List[str] | str]], Dict[str, List[Dict[str, str]]]]:
    """Load market_config.json with validation and merge onto built-in defaults."""
    market_countries = deepcopy(_DEFAULT_MARKET_COUNTRIES)
    competitor_platforms = deepcopy(_DEFAULT_COMPETITOR_PLATFORMS)

    if not os.path.exists(path):
        logger.warning(f"Market config file not found at '{path}'. Using built-in defaults.")
        return market_countries, competitor_platforms

    with open(path, "r") as f:
        raw = json.load(f)

    if not isinstance(raw, dict):
        raise ValueError("market_config.json must be a top-level object")

    raw_countries = raw.get("market_countries")
    if isinstance(raw_countries, dict):
        for canonical, meta in raw_countries.items():
            canonical_norm = str(canonical).upper().strip()
            if canonical_norm and _is_valid_market_country_entry(meta):
                market_countries[canonical_norm] = {
                    "gl": str(meta["gl"]).lower().strip(),
                    "aliases": [str(a).upper().strip() for a in meta["aliases"] if str(a).strip()],
                }
            else:
                logger.warning(f"Skipping invalid market country entry for '{canonical}'.")

    alias_lookup, _ = _build_country_lookup_maps(market_countries)

    raw_platforms = raw.get("competitor_platforms")
    if isinstance(raw_platforms, dict):
        for country_key, entries in raw_platforms.items():
            country_norm = str(country_key).upper().strip()
            canonical = alias_lookup.get(country_norm, country_norm)
            if not isinstance(entries, list):
                logger.warning(f"Skipping invalid competitor platforms list for '{country_key}'.")
                continue

            normalized_entries: List[Dict[str, str]] = []
            for entry in entries:
                if _is_valid_competitor_platform_entry(entry):
                    normalized_entries.append(
                        {
                            "name": str(entry["name"]).strip(),
                            "key": str(entry["key"]).strip(),
                            "site": str(entry["site"]).strip(),
                        }
                    )
                else:
                    logger.warning(f"Skipping invalid competitor platform entry for '{country_key}'.")

            if normalized_entries:
                competitor_platforms[canonical] = normalized_entries

    return market_countries, competitor_platforms

_MARKET_COUNTRIES: Dict[str, Dict[str, List[str] | str]] = deepcopy(_DEFAULT_MARKET_COUNTRIES)

_COUNTRY_ALIASES: Dict[str, str] = {
    alias: canonical
    for canonical, meta in _MARKET_COUNTRIES.items()
    for alias in meta["aliases"]
}
_COUNTRY_GL_MAP: Dict[str, str] = {
    canonical: str(meta["gl"])
    for canonical, meta in _MARKET_COUNTRIES.items()
}


def _canonical_country_code(country: Optional[str]) -> str:
    """Resolve country aliases to supported canonical 2-letter codes."""
    norm = str(country or "US").upper().strip()
    return _COUNTRY_ALIASES.get(norm, norm)


def _get_known_cuisines() -> List[str]:
    """Return configured cuisine list or built-in fallback."""
    return QUAL_RULES.get("cuisine_keywords") or _BUILTIN_CUISINES


def _normalize_cuisine_key(value: str) -> str:
    """Normalize cuisine labels for robust comparisons."""
    return re.sub(r'\s+', ' ', normalize_string(value)).strip()


def _build_cuisine_lookup(cuisines: List[str]) -> Dict[str, str]:
    """Map normalized cuisine variants to canonical labels."""
    lookup: Dict[str, str] = {}
    for cuisine in cuisines:
        key = _normalize_cuisine_key(cuisine)
        if key:
            lookup[key] = cuisine
    return lookup


def _clean_google_type_label(value: str) -> str:
    """Normalize Google Maps type labels / IDs into display-friendly text."""
    lowered = value.lower().replace('_', ' ').strip()
    suffixes = [" restaurant", " takeaway", " food", " cuisine", " place"]
    changed = True
    while changed:
        changed = False
        for suffix in suffixes:
            if lowered.endswith(suffix):
                lowered = lowered[:-len(suffix)].strip()
                changed = True
    lowered = re.sub(r'\s+', ' ', lowered).strip()
    return lowered.title()


def _match_known_cuisine(
    candidates: List[str],
    cuisine_lookup: Dict[str, str],
) -> Optional[str]:
    """Return canonical cuisine for the best matching candidate."""
    for candidate in candidates:
        key = _normalize_cuisine_key(candidate)
        if key and key in cuisine_lookup:
            return cuisine_lookup[key]
    return None


def _extract_visible_text(html: str) -> str:
    """Strip HTML/script/style noise before keyword cuisine detection."""
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.I | re.S)
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.I | re.S)
    text = re.sub(r'<[^>]+>', ' ', text)
    return re.sub(r'\s+', ' ', text)


_DEFAULT_COMPETITOR_PLATFORMS: Dict[str, List[Dict[str, str]]] = {
    # UK: both Uber Eats and Deliveroo are active
    "UK": [
        {"name": "Uber Eats", "key": "uber_eats_url", "site": "ubereats.com"},
        {"name": "Deliveroo",  "key": "deliveroo_url",  "site": "deliveroo.co.uk"},
    ],
    # NL: Deliveroo exited in 2022 — Uber Eats only
    "NL": [
        {"name": "Uber Eats", "key": "uber_eats_url", "site": "ubereats.com"},
    ],
    # DE: Deliveroo exited — Uber Eats only
    "DE": [
        {"name": "Uber Eats", "key": "uber_eats_url", "site": "ubereats.com"},
    ],
    # AT: Uber Eats only
    "AT": [
        {"name": "Uber Eats", "key": "uber_eats_url", "site": "ubereats.com"},
    ],
    # BE: both active
    "BE": [
        {"name": "Uber Eats", "key": "uber_eats_url", "site": "ubereats.com"},
        {"name": "Deliveroo",  "key": "deliveroo_url",  "site": "deliveroo.be"},
    ],
    # CH: Uber Eats only
    "CH": [
        {"name": "Uber Eats", "key": "uber_eats_url", "site": "ubereats.com"},
    ],
    # US: Uber Eats only (DoorDash has no public-index restaurant pages suitable for site: filter)
    "US": [
        {"name": "Uber Eats", "key": "uber_eats_url", "site": "ubereats.com"},
    ],
}

# --- Thread-safe FSA response cache ---
# Key: (normalized_name, postcode) → list[establishment dicts]
# This prevents redundant FSA calls when multiple leads share the same address.
_fsa_cache: Dict[tuple, list] = {}
_fsa_cache_lock = threading.Lock()

# Pre-compiled Regex for Performance
POSTAL_REGEX = re.compile(r'([A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}|\b\d{4,5}(?:[-\s][A-Z]{1,2})?\b)', re.I)

# Constants
GENERIC_CUISINE_TYPES = {'restaurant', 'food', 'point_of_interest', 'establishment', 'store', 'meal_takeaway', 'meal_delivery'}

# Meal-period / service-style type IDs that describe *when* food is served, not its culinary origin.
# These are deprioritised so that ethnic cuisine types (turkish_restaurant, italian_restaurant …)
# always win when both are present on a Google Maps listing.
_MEAL_TYPE_IDS: set = {
    'breakfast_restaurant', 'brunch_restaurant', 'lunch_restaurant',
    'dinner_restaurant', 'fast_food_restaurant', 'buffet_restaurant',
    'sandwich_shop', 'ice_cream_shop', 'dessert_restaurant', 'dessert_shop',
    'snack_bar', 'diner',
}
# Venue type IDs used as last resort when no recognised cuisine is found
_VENUE_TYPE_IDS: set = {'cafe', 'bar', 'pub', 'bakery', 'coffee_shop', 'fast_food'}

# Known cuisine keywords — the single source of truth for what counts as a valid cuisine.
# Add new cuisines here or override via qualification_config.json → rules.cuisine_keywords.
_BUILTIN_CUISINES: List[str] = [
    "Italian", "Mexican", "Chinese", "Japanese", "Indian", "Thai", "French",
    "Spanish", "Greek", "Mediterranean", "Middle Eastern", "Korean", "Vietnamese",
    "American", "British", "Pizza", "Burger", "Sushi", "Seafood", "Vegan",
    "Vegetarian", "BBQ", "Steakhouse", "Turkish", "Lebanese", "Caribbean",
    "Persian", "Moroccan", "Filipino", "Indonesian", "Brazilian", "Halal",
]

# Minimum fuzzy score (0–100) required to accept a name match from local_results or FSA.
# Scores below this mean the result is too different from the Salesforce Company name.
MIN_MATCH_SCORE = 60

# --- Qualification config schema (required keys with their expected types) ---
_CONFIG_SCHEMA: Dict[str, type] = {
    "require_delivery_for_types": list,
    "require_delivery_for_name_keywords": list,
    "always_disqualify_types": list,
    "residential_signals": list,
    "residential_exception_types": list,
    "pre_qualification_rules": dict,
    "cuisine_keywords": list,
}

_CONFIG_DEFAULTS: Dict[str, Any] = {
    "require_delivery_for_types": [],
    "require_delivery_for_name_keywords": [],
    "always_disqualify_types": [],
    "residential_signals": [],
    "residential_exception_types": [],
    "pre_qualification_rules": {"disqualify_name_keywords": [], "disqualify_address_keywords": []},
    "cuisine_keywords": [],  # empty → determine_cuisine_from_text uses its built-in list
}


def _load_and_validate_config(path: str) -> Dict[str, Any]:
    """Load qualification_config.json and validate it against the known schema.

    Raises a clear ValueError if required keys are missing or have the wrong type.
    Falls back to safe defaults for any missing key so the pipeline can still run.
    """
    if not os.path.exists(path):
        logger.warning(f"Config file not found at '{path}'. Using built-in defaults.")
        return _CONFIG_DEFAULTS.copy()

    with open(path, 'r') as f:
        raw = json.load(f)

    rules = raw.get('rules')
    if not isinstance(rules, dict):
        raise ValueError(
            f"qualification_config.json must have a top-level 'rules' dict. Got: {type(rules).__name__}"
        )

    validated: Dict[str, Any] = {}
    schema_errors: List[str] = []
    for key, expected_type in _CONFIG_SCHEMA.items():
        value = rules.get(key)
        if value is None:
            logger.warning(f"Config key '{key}' is missing — using default.")
            validated[key] = _CONFIG_DEFAULTS[key]
        elif not isinstance(value, expected_type):
            schema_errors.append(
                f"  '{key}': expected {expected_type.__name__}, got {type(value).__name__}"
            )
            validated[key] = _CONFIG_DEFAULTS[key]
        else:
            validated[key] = value

    if schema_errors:
        logger.error(
            "qualification_config.json has schema errors (defaults applied):\n" + "\n".join(schema_errors)
        )

    return validated


# Load Qualification Rules
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'qualification_config.json')
try:
    QUAL_RULES = _load_and_validate_config(CONFIG_PATH)
except Exception as e:
    logger.error(f"Failed to load qualification rules: {e}")
    QUAL_RULES = _CONFIG_DEFAULTS.copy()


# Load Market Rules
MARKET_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'market_config.json')
try:
    _MARKET_COUNTRIES, _COMPETITOR_PLATFORMS = _load_market_config(MARKET_CONFIG_PATH)
except Exception as e:
    logger.error(f"Failed to load market rules: {e}")
    _MARKET_COUNTRIES = deepcopy(_DEFAULT_MARKET_COUNTRIES)
    _COMPETITOR_PLATFORMS = deepcopy(_DEFAULT_COMPETITOR_PLATFORMS)

_COUNTRY_ALIASES, _COUNTRY_GL_MAP = _build_country_lookup_maps(_MARKET_COUNTRIES)

def search_competitor_links(
    business_name: str,
    city: Optional[str],
    postcode: Optional[str],
    country_code: str,
) -> Dict[str, Optional[str]]:
    """Search DuckDuckGo for competitor platform listings for a given business.

    Uses ``site:`` filter queries (e.g. ``site:ubereats.com``) combined with the
    business name and location.  No API key is required — DuckDuckGo is free.
    A threading semaphore caps concurrent calls to avoid triggering rate limits.

    Args:
        business_name: Restaurant / business name to look up.
        city: City name for narrowing the search (used if postcode is absent).
        postcode: Postcode for narrowing the search.
        country_code: Raw country code from the lead (e.g. "UK", "GB", "NL").

    Returns:
        Dict mapping platform keys (``uber_eats_url``, ``deliveroo_url``, …) to
        the best-matching URL string or ``None`` when not found.

    Note:
        Thuisbezorgd and Lieferando are Just Eat brands and are intentionally
        excluded from the competitor list.
    """
    if not _DDG_AVAILABLE or not business_name:
        return {}

    # Normalise country code to the canonical key used in _COMPETITOR_PLATFORMS
    country_norm = _canonical_country_code(country_code)
    platforms = _COMPETITOR_PLATFORMS.get(country_norm, _COMPETITOR_PLATFORMS.get("US", []))

    # Pre-initialise all expected keys so callers always see a consistent shape
    found: Dict[str, Optional[str]] = {p["key"]: None for p in platforms}

    norm_name = normalize_string(business_name)

    for platform in platforms:
        site = platform["site"]
        key  = platform["key"]
        name = platform["name"]

        # Build candidate queries: precise (with postcode) first, then city-only fallback.
        # Uber Eats / Deliveroo pages often don't surface the postcode in indexed text,
        # so the city-only query is the more reliable signal.
        queries: List[str] = []
        if postcode:
            queries.append(f'site:{site} "{business_name}" "{postcode}"')
        if city and city != postcode:
            queries.append(f'site:{site} "{business_name}" "{city}"')
        queries.append(f'site:{site} "{business_name}"')  # bare fallback

        for query in queries:
            if found[key]:  # already resolved by a previous attempt
                break
            try:
                with _ddg_semaphore:
                    time.sleep(_DDG_SLEEP)
                    raw_results = list(_DDGS().text(query, max_results=3))

                for r in raw_results or []:
                    url = r.get("href", "")
                    if not url or site not in url:
                        continue

                    # Validate: strip platform suffix from title, then fuzzy-match
                    title = r.get("title", "")
                    cleaned_title = re.sub(r'\s*[\|\u2013\u2014\-].*$', '', title).strip()
                    score = max(
                        fuzz.token_set_ratio(norm_name, normalize_string(cleaned_title)),
                        fuzz.partial_ratio(norm_name, normalize_string(cleaned_title)),
                    )
                    if score >= 45:
                        found[key] = url
                        logger.debug(f"{name} link found for '{business_name}' (query='{query}'): {url} (score={score})")
                        break

            except Exception as exc:
                logger.warning(f"DDG competitor search failed for '{business_name}' on {name}: {exc}")
                break  # don't retry on exception

        if not found[key]:
            logger.debug(f"No {name} listing found for '{business_name}'")

    return found


def _serpapi_fetch_with_retry(search_params: dict, retries: int = 3, delay: float = 1.5) -> dict:
    """Call SerpAPI with exponential-backoff retry on rate-limit errors.

    Defined at module level so it is created once per process, not once per lead.
    """
    for attempt in range(retries):
        try:
            res = GoogleSearch(search_params).get_dict()
            if "error" in res and "rate limit" in str(res.get("error")).lower():
                wait = delay * (2 ** attempt)
                logger.warning(f"SerpAPI rate limit hit. Retrying in {wait:.1f}s...")
                time.sleep(wait)
                continue
            return res
        except Exception as ex:
            if attempt == retries - 1:
                raise
            time.sleep(delay * (2 ** attempt))
    return {}


def extract_address_component_types(place: Dict[str, Any]) -> List[str]:
    """Extract normalized Google address component type identifiers.

    SerpAPI may expose `address_components` as a list of dicts where each item
    contains a `types` list. These type IDs are used by residential signals in
    qualification config (e.g. `subpremise`, `street_address`).
    """
    components = place.get("address_components")
    if not isinstance(components, list):
        return []

    collected: List[str] = []
    for comp in components:
        if not isinstance(comp, dict):
            continue
        comp_types = comp.get("types")
        if not isinstance(comp_types, list):
            continue
        for comp_type in comp_types:
            if not comp_type:
                continue
            t = str(comp_type).lower().strip()
            if t and t not in collected:
                collected.append(t)
    return collected


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

def determine_cuisine_from_text(text: str) -> Optional[str]:
    """Scan text for cuisine keywords and return the most-mentioned one.

    The keyword list is loaded from qualification_config.json under
    ``rules.cuisine_keywords``. The built-in list is the fallback
    used when the config key is absent, so no code change is needed to
    add new cuisines — just edit the JSON file.
    """
    cuisines = _get_known_cuisines()
    cuisine_lookup = _build_cuisine_lookup(cuisines)
    text_normalized = _normalize_cuisine_key(text)
    if not text_normalized:
        return None

    counts: Dict[str, int] = {}
    for key, canonical in cuisine_lookup.items():
        if not key:
            continue
        pattern = rf'\b{re.escape(key)}\b'
        matches = re.findall(pattern, text_normalized)
        if matches:
            counts[canonical] = counts.get(canonical, 0) + len(matches)

    if counts:
        return max(counts.items(), key=lambda item: item[1])[0]
    return None

def sanitize_string(value: Any, max_length: int = 255) -> Optional[str]:
    """Strip control characters, collapse whitespace, and truncate for Salesforce text fields."""
    if value is None:
        return None
    s = str(value)
    # Remove non-printable / control characters (keep newlines for Long Text Areas)
    s = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', s)
    s = re.sub(r'[ \t]+', ' ', s).strip()
    return s[:max_length] if s else None


def fetch_menu_and_determine_cuisine(menu_link: Optional[str], website: Optional[str]) -> Optional[str]:
    """Fetch menu/website page and infer cuisine from its text content.

    Uses the shared _http_session for connection pooling.
    """
    urls_to_try = [url for url in [menu_link, website] if url]
    for url in urls_to_try:
        try:
            res = _http_session.get(url, timeout=5)
            if res.status_code == 200:
                cuisine = determine_cuisine_from_text(_extract_visible_text(res.text))
                if cuisine:
                    return cuisine
        except Exception:
            pass
    return None

# Salesforce Long Text Area fields can hold up to 32,768 characters.
# Opening_Hours__c is a Long Text Area so we use the full limit instead of
# the 255-char limit applied to regular Text fields.
_SF_LONG_TEXT_MAX = 32_768


def extract_opening_hours(place: Dict[str, Any]) -> Optional[str]:
    hours = place.get("hours")

    # Handle alternative SerpAPI formats just in case
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
    return result[:_SF_LONG_TEXT_MAX] if result else None

def extract_cuisines(place: Dict[str, Any]) -> Dict[str, Optional[str]]:
    # SerpAPI returns parallel arrays: type (display names) and type_ids (snake_case IDs).
    # Use type_ids for filtering against GENERIC_CUISINE_TYPES (which uses IDs),
    # and the corresponding type display name for human-readable output.
    raw_type_ids = place.get("type_ids", [])
    raw_type_names = place.get("type", [])

    if not isinstance(raw_type_ids, list):
        raw_type_ids = []
    if not isinstance(raw_type_names, list):
        raw_type_names = []

    # Pair each ID with its display name; derive IDs from names when type_ids is absent
    # (e.g. thin local_results cards don't include type_ids).
    if raw_type_ids:
        names = raw_type_names if len(raw_type_names) == len(raw_type_ids) else raw_type_ids
        type_pairs = list(zip(raw_type_ids, names))
    else:
        type_pairs = [(t.lower().replace(' ', '_'), t) for t in raw_type_names]

    known_cuisines = _get_known_cuisines()
    cuisine_lookup = _build_cuisine_lookup(known_cuisines)
    known_lower: set = {c.lower() for c in known_cuisines}

    # Classify each Google Maps type into one of three buckets (priority high → low):
    #   confirmed  — type name matches a known cuisine keyword (e.g. Turkish, Italian)
    #   other      — non-generic, non-meal-period type that is NOT a known cuisine (e.g. Family)
    #   meal_types — describes when food is served, not what cuisine it is (e.g. Breakfast)
    confirmed: List[str] = []
    other: List[str] = []
    meal_types: List[str] = []
    for type_id, display_name in type_pairs:
        tid = type_id.lower()
        if tid in GENERIC_CUISINE_TYPES:
            continue
        cleaned_display = _clean_google_type_label(display_name)
        cleaned_id = _clean_google_type_label(type_id)
        cleaned = cleaned_display or cleaned_id
        if not cleaned:
            continue

        matched_cuisine = _match_known_cuisine(
            candidates=[cleaned_display, cleaned_id, display_name, type_id],
            cuisine_lookup=cuisine_lookup,
        )

        if matched_cuisine:
            if matched_cuisine not in confirmed:
                confirmed.append(matched_cuisine)
        elif tid in _MEAL_TYPE_IDS:
            if cleaned not in meal_types:
                meal_types.append(cleaned)
        else:
            if cleaned not in other:
                other.append(cleaned)

    # Use highest-priority non-empty bucket
    cuisines = confirmed or other or meal_types

    # Last resort: venue type IDs (cafe, bar, bakery …) when all buckets are empty
    if not cuisines:
        for type_id, display_name in type_pairs:
            if type_id.lower() in _VENUE_TYPE_IDS:
                cleaned = display_name.replace('_', ' ').title().strip()
                if cleaned not in cuisines:
                    cuisines.append(cleaned)

    primary_cuisine = cuisines[0] if cuisines else None
    secondary_cuisine = cuisines[1] if len(cuisines) > 1 else None

    # Menu/Website Fallback: SerpAPI exposes the menu link at place["menu"]["link"]
    menu_link = place.get("menu_link") or place.get("menu", {}).get("link")
    website = place.get("website")

    # Scrape when primary is absent OR is not a recognised cuisine keyword.
    # This catches vague types like "Family" or "International" that slip through
    # when Google Maps has no explicit ethnic cuisine type.
    if not primary_cuisine or primary_cuisine.lower() not in known_lower:
        scraped_cuisine = fetch_menu_and_determine_cuisine(menu_link, website)
        if scraped_cuisine:
            primary_cuisine = scraped_cuisine
            if scraped_cuisine not in cuisines:
                cuisines.insert(0, scraped_cuisine)

    # Recompute secondary from the final cuisines list — scraping may have shifted positions
    secondary_cuisine = cuisines[1] if len(cuisines) > 1 else None

    return {
        "primary_cuisine": primary_cuisine[:255] if primary_cuisine else None,
        "secondary_cuisine": secondary_cuisine[:255] if secondary_cuisine else None,
    }

def safe_isna(val: Any) -> bool:
    """Return True if val is None or a pandas NA/NaN scalar.

    Unlike pd.isna(), this never raises a TypeError on non-scalar types
    (e.g. lists or dicts stored in a DataFrame cell).
    """
    if val is None:
        return True
    try:
        return bool(pd.isna(val))
    except (TypeError, ValueError):
        return False


def extract_address_components(place: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Parse address components from a Google Maps full address string.

    Google Maps formats addresses differently per market:

      DE / NL / AT / CH / BE  — postcode and city share the second-to-last segment:
        "Unter den Linden 1, 10117 Berlin, Germany"
        "Kalverstraat 1, 1012 NX Amsterdam, Netherlands"

      UK  — postcode is its OWN segment, city comes one segment earlier:
        "123 High St, York, YO1 9AB, United Kingdom"

    Parsing strategy:
      parts[0]  → street
      parts[-1] → country
      parts[-2] → try postcode + city together (DE/NL style);
                  if the whole segment is just a postcode, city = parts[-3] (UK style)
    """
    full_address = place.get("address", "")
    res = {"street": None, "city": None, "postal_code": None, "country": None}
    if not full_address:
        return res

    parts = [p.strip() for p in full_address.split(",")]

    res["street"] = parts[0] if parts else None

    if len(parts) >= 2:
        res["country"] = parts[-1]

    if len(parts) >= 3:
        city_postal_part = parts[-2]
        postal_match = POSTAL_REGEX.search(city_postal_part)
        if postal_match:
            res["postal_code"] = postal_match.group().strip()
            city_candidate = city_postal_part.replace(postal_match.group(), "").strip().strip(",").strip()
            if city_candidate:
                # DE/NL style: "10117 Berlin" → city = "Berlin"
                res["city"] = city_candidate
            elif len(parts) >= 4:
                # UK style: "YO1 9AB" consumed the whole segment; city is parts[-3]
                # e.g. "123 High St, York, YO1 9AB, United Kingdom" → city = "York"
                res["city"] = parts[-3]
        else:
            res["city"] = city_postal_part
    # len == 2: only "Street, Country" — no city/postcode to extract

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
    google_address: Optional[str] = None,
    address_component_types: Optional[List[str]] = None,
) -> Tuple[str, Optional[str]]:
    """Qualification logic with strict Name and Address verification."""
    norm_sf_name = normalize_string(sf_company)
    norm_google_name = normalize_string(google_name)
    norm_types = [t.lower() for t in types]
    norm_address_component_types = [str(t).lower() for t in (address_component_types or []) if t]
    service_options = service_options if isinstance(service_options, dict) else {}
    
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
        
        # Fuzzy match for street
        street_match = False
        if norm_sf_street:
            # partial_ratio is good because '123 main st' is a substring of '123 main street'
            match_score = max(fuzz.partial_ratio(norm_sf_street, norm_google_addr), 
                              fuzz.token_set_ratio(norm_sf_street, norm_google_addr))
            if match_score > 75:
                street_match = True
                
        # Substring or Fuzzy match for postcode
        postcode_match = False
        if norm_sf_postcode:
            if norm_sf_postcode in norm_google_addr or fuzz.partial_ratio(norm_sf_postcode, norm_google_addr) > 85:
                postcode_match = True
        
        if not street_match and not postcode_match:
            return "Disqualified", f"Automation Disqualified: Location Mismatch (SF Address: '{sf_street} {sf_postcode}', Google: '{google_address}')."

    # 3. Name Match Check
    if norm_google_name and norm_sf_name:
        match_score = max(
            fuzz.token_set_ratio(norm_sf_name, norm_google_name),
            fuzz.partial_ratio(norm_sf_name, norm_google_name)
        )
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
    has_residential_signal = any(
        signal in norm_types or signal in norm_address_component_types
        for signal in res_signals
    )
    has_exception = any(
        ex in norm_types or ex in norm_address_component_types
        for ex in res_exceptions
    )
    if has_residential_signal and not has_exception:
        return "Disqualified", "Automation Disqualified: Residential Address detected."
    
    # 6. Service Delivery Check
    req_types = QUAL_RULES.get('require_delivery_for_types', [])
    req_keywords = QUAL_RULES.get('require_delivery_for_name_keywords', [])
    combined_name = f"{sf_company} {google_name or ''}".lower()
    is_target = any(t in norm_types for t in req_types) or any(k in combined_name for k in req_keywords)
    
    if is_target and service_options.get("delivery") is False:
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
        canonical = _canonical_country_code(self.country_code)
        return _COUNTRY_GL_MAP.get(canonical, "us")

    def enrich(self, lead_row: pd.Series) -> Optional[Dict[str, Any]]:
        """Enrichment using SerpAPI with two strategies:

        1. Place ID lookup (exact — preferred when Google_Place_ID__c is set)
        2. Name + address text search with fuzzy best-match selection
        """
        place_id = lead_row.get("Google_Place_ID__c")
        name = lead_row.get("Company") or lead_row.get("Name")
        country = lead_row.get("Country")

        if not place_id and not name:
            return None

        params = {"engine": "google_maps", "api_key": self.api_key, "gl": self._get_gl()}

        # Strategy 1: Exact Place ID lookup — most reliable, no fuzzy matching needed
        if place_id:
            params["place_id"] = place_id
            try:
                results = _serpapi_fetch_with_retry(params)
                if place := results.get("place_results"):
                    return self._process_results(place, lead_row)
            except Exception as e:
                logger.warning(f"Place ID search failed for {place_id}: {e}")

        # Strategy 2: Name + full address text search
        # Include Street, City, and PostalCode for a "unique signature" query
        addr_parts = [
            p for p in [lead_row.get("Street"), lead_row.get("City"), lead_row.get("PostalCode")]
            if p and not safe_isna(p)
        ]
        loc_context = ", ".join(addr_parts) or country or ""
        search_query = f"{name} {loc_context}".strip()

        params.pop("place_id", None)
        params["q"] = search_query
        try:
            results = _serpapi_fetch_with_retry(params)
            if place := results.get("place_results"):
                return self._process_results(place, lead_row)

            elif local := results.get("local_results"):
                # Pick the best fuzzy name match from local results.
                # Enforce MIN_MATCH_SCORE so we don't accept a completely wrong business.
                norm_name = normalize_string(name)
                best_match = None
                best_score = 0
                for r in local:
                    score = max(
                        fuzz.token_set_ratio(norm_name, normalize_string(r.get("title", ""))),
                        fuzz.partial_ratio(norm_name, normalize_string(r.get("title", ""))),
                    )
                    if score > best_score:
                        best_score = score
                        best_match = r

                if best_match and best_score >= MIN_MATCH_SCORE:
                    logger.debug(f"Best local result for '{name}': '{best_match.get('title')}' (score={best_score})")

                    # Strategy 2b: Upgrade to a full Place ID lookup using the
                    # place_id embedded in the local result. Local results are
                    # "summary cards" — they omit service_options, opening_hours,
                    # website, payment_options, etc. A follow-up Place ID call
                    # fetches the complete detail record for free (same API credit).
                    matched_place_id = best_match.get("place_id") or best_match.get("data_id")
                    if matched_place_id:
                        try:
                            detail_params = dict(params)  # copy; don't mutate shared params
                            detail_params.pop("q", None)
                            detail_params["place_id"] = matched_place_id
                            detail_results = _serpapi_fetch_with_retry(detail_params)
                            if full_place := detail_results.get("place_results"):
                                logger.info(
                                    f"Upgraded local result to full Place ID detail for '{name}' "
                                    f"(place_id={matched_place_id})"
                                )
                                return self._process_results(full_place, lead_row)
                        except Exception as e:
                            logger.warning(
                                f"Place ID upgrade failed for '{matched_place_id}': {e}. "
                                f"Falling back to thin local result."
                            )

                    # Fallback: parse whatever the local result contains
                    return self._process_results(best_match, lead_row)
                else:
                    logger.warning(
                        f"No local result met MIN_MATCH_SCORE ({MIN_MATCH_SCORE}) for '{name}'. "
                        f"Best was '{best_match.get('title') if best_match else 'N/A'}' (score={best_score})."
                    )
        except Exception as e:
            logger.error(f"Fallback search failed for '{search_query}': {e}")

        return None

    def _process_results(self, place: Dict[str, Any], lead_row: pd.Series) -> Dict[str, Any]:
        """Common logic to parse SerpAPI results."""
        addr = extract_address_components(place)
        address_component_types = extract_address_component_types(place)
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
            "primary_cuisine": cuisines["primary_cuisine"],
            "secondary_cuisine": cuisines["secondary_cuisine"],
            "opening_hours": extract_opening_hours(place),
            **addr,
            "full_address": place.get("address"),
            "raw_address_component_types": address_component_types,
            "raw_types": place.get("type_ids", place.get("type", [])),  # prefer IDs for qualification checks
            "raw_service_options": place.get("service_options", {}),
            "business_status": place.get("business_status"),
            "permanently_closed": place.get("permanently_closed", False)
        }
        return self.post_enrich(data, lead_row)

    def post_enrich(self, data: Dict[str, Any], lead_row: pd.Series) -> Dict[str, Any]:
        """Hook for market-specific extra steps. Base implementation adds competitor links."""
        business_name = data.get("google_name") or lead_row.get("Company") or ""
        if business_name:
            competitor_links = search_competitor_links(
                business_name=business_name,
                city=data.get("city") or lead_row.get("City"),
                postcode=data.get("postal_code") or lead_row.get("PostalCode"),
                country_code=self.country_code,
            )
            data.update(competitor_links)
        return data

class UKMarketHandler(MarketHandler):
    """UK-specific enrichment adding FSA Hygiene data.

    FSA calls are cached per (normalized_name, postcode) pair using the module-level
    _fsa_cache dict protected by _fsa_cache_lock, so concurrent threads processing
    leads at the same address never make duplicate HTTP calls.
    """

    def _fetch_fsa_establishments(self, search_name: str, postcode: Optional[str]) -> list:
        """Return FSA establishments, consulting the thread-safe cache first."""
        cache_key = (search_name, postcode or "")

        with _fsa_cache_lock:
            if cache_key in _fsa_cache:
                logger.debug(f"FSA cache hit for {cache_key}")
                return _fsa_cache[cache_key]

        headers = {"x-api-version": "2", "Accept": "application/json"}

        def _get_with_retry(query_params: dict) -> Optional[requests.Response]:
            """GET the FSA /Establishments endpoint with exponential-backoff on 429."""
            for attempt in range(3):
                try:
                    resp = _http_session.get(
                        f"{FSA_API_BASE}/Establishments",
                        params=query_params,
                        headers=headers,
                        timeout=5,
                    )
                    if resp.status_code == 429:
                        wait = 2 ** attempt
                        logger.warning(f"FSA rate-limited. Retrying in {wait}s...")
                        time.sleep(wait)
                        continue
                    return resp
                except requests.exceptions.RequestException as exc:
                    logger.warning(f"FSA request error (attempt {attempt + 1}): {exc}")
                    time.sleep(2 ** attempt)
            return None

        establishments: list = []
        params = {"name": search_name, "address": postcode} if postcode else {"name": search_name}
        res = _get_with_retry(params)
        if res and res.status_code == 200:
            establishments = res.json().get("establishments", [])

        # Fallback: postcode-only search + local fuzzy filter
        if not establishments and postcode:
            res_pc = _get_with_retry({"address": postcode})
            if res_pc and res_pc.status_code == 200:
                for e in res_pc.json().get("establishments", []):
                    b_name = normalize_string(e.get("BusinessName", ""))
                    if (
                        fuzz.token_set_ratio(search_name, b_name) >= 70
                        or fuzz.partial_ratio(search_name, b_name) >= 75
                    ):
                        establishments.append(e)

        with _fsa_cache_lock:
            _fsa_cache[cache_key] = establishments

        return establishments

    def post_enrich(self, data: Dict[str, Any], lead_row: pd.Series) -> Dict[str, Any]:
        # Run base enrichment first (adds competitor links)
        data = super().post_enrich(data, lead_row)

        raw_name = data.get("google_name") or lead_row.get("Company")
        postcode = data.get("postal_code") or lead_row.get("PostalCode")

        if not raw_name:
            return data

        # FSA API is picky with accents — normalize for better matching
        search_name = normalize_string(raw_name)

        try:
            establishments = self._fetch_fsa_establishments(search_name, postcode)

            if establishments:
                # Pick the best fuzzy match and enforce a minimum confidence threshold.
                # Without this, a business at the same postcode but with a completely
                # different name could supply wrong hygiene data.
                best_match = None
                best_score = 0
                for e in establishments:
                    score = max(
                        fuzz.token_set_ratio(search_name, normalize_string(e.get("BusinessName", ""))),
                        fuzz.partial_ratio(search_name, normalize_string(e.get("BusinessName", ""))),
                    )
                    if score > best_score:
                        best_score = score
                        best_match = e

                if best_match and best_score >= MIN_MATCH_SCORE:
                    rating_map = {
                        "0": "ZERO", "1": "ONE", "2": "TWO",
                        "3": "THREE", "4": "FOUR", "5": "FIVE",
                    }
                    raw_rating_val = best_match.get("RatingValue")
                    fsa_update = {
                        "FSA_AGENCY": best_match.get("LocalAuthorityName"),
                        "FSA_URL": f"https://ratings.food.gov.uk/business/en-GB/{best_match.get('FHRSID')}",
                    }
                    if raw_rating_val is not None:
                        mapped = rating_map.get(str(raw_rating_val))
                        if mapped:
                            fsa_update["FSA_RATING"] = mapped
                        else:
                            logger.debug(
                                f"FSA RatingValue '{raw_rating_val}' has no Salesforce picklist mapping — FSA_RATING skipped."
                            )
                    data.update(fsa_update)
                    logger.info(f"FSA match for '{raw_name}': '{best_match.get('BusinessName')}' (score={best_score})")
                else:
                    logger.warning(
                        f"FSA match too weak for '{raw_name}' — best was "
                        f"'{best_match.get('BusinessName') if best_match else 'N/A'}' "
                        f"(score={best_score}, threshold={MIN_MATCH_SCORE}). Skipping FSA data."
                    )
            else:
                logger.info(f"No FSA establishments found for '{search_name}' in '{postcode}'")

        except requests.exceptions.RequestException as e:
            logger.error(f"FSA API request error: {e}")
        except Exception as e:
            logger.error(f"FSA parsing error: {e}")

        return data

class MarketFactory:
    """Factory to return the appropriate MarketHandler."""

    @classmethod
    def get_handler(cls, country: Optional[str], api_key: str) -> MarketHandler:
        country_norm = _canonical_country_code(country)
        if country_norm == "UK":
            return UKMarketHandler(country_norm, api_key)
        return MarketHandler(country_norm, api_key)
