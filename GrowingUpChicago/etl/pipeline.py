
from datetime import datetime, timedelta
from etl.events_transformer import EventTransformer
from etl.database_manager import DatabaseManager
from etl import utils, extract_helpers
from etl.price import PriceCategorizer

import pandas as pd
import re
from typing import List, Dict, Optional

# ==================== EXTRACT ====================

def extract_sources(cfg: Dict) -> List[Dict]:
    """
    Extract raw data from various sources (APIs, web scraping, etc.)
    
    Args:
        cfg: Configuration dict containing source endpoints, credentials, etc.
        
    Returns:
        List of raw event dictionaries with source metadata
    """    
    extracted_events = []

    print(f"Extracted {len(extracted_events)} raw events from all sources")
    return extracted_events

def _extract_from_pl_api(config: Dict) -> List[Dict]:
    """Extract from CHICAGO PL CSV"""
    # For Public Library, extract lat/lng from the 'Location' POINT string
    def extract_coords(point_str):
        if pd.isna(point_str): return None, None
        match = re.search(r'POINT \((-?\d+\.\d+) (-?\d+\.\d+)\)', str(point_str))
        return (float(match.group(2)), float(match.group(1))) if match else (None, None)
        
    pl_df = pd.read_csv(config.get('url', ''))
    if 'Location' in pl_df.columns:
        pl_df[['latitude', 'longitude']] = pl_df['Location'].apply(lambda x: pd.Series(extract_coords(x)))
    pl_df["source_type"] = "pl"
    return pl_df.to_dict(orient="records")

def _extract_from_pd_api(config: Dict) -> List[Dict]:
    """Extract from CHICAGO PD API"""
    pd_df = extract_helpers.read_csv_api(config.get('url', ''))
    pd_df = pd_df[pd_df["type"] == "Event"]
    pd_df["source_type"] = "pd"
    return pd_df.to_dict(orient="records")

def _extract_from_my_chi_api(config: Dict) -> List[Dict]:
    """Extract from My Chi API"""
    my_chi_df = extract_helpers.read_csv_api(config.get('url', ''))
    my_chi_df = my_chi_df[(pd.to_datetime(my_chi_df["end_date_and_time"]) - pd.to_datetime(my_chi_df["start_date_and_time"])).dt.days <= 1]
    my_chi_df["source_type"] = "my_chi"
    return my_chi_df.to_dict(orient="records")

def extract_chicago_apis(configs: List[Dict]) -> List[Dict]:
    """Extracts from all configured Chicago Open Data APIs."""
    all_events = []
    for config in configs:
        source_type = config['source_type']
        print(f"Extracting from Chicago API: {source_type}")
        if source_type == "pl":
            all_events.extend(_extract_from_pl_api(config))
        elif source_type == "pd":
            all_events.extend(_extract_from_pd_api(config))
        elif source_type == "my_chi":
            all_events.extend(_extract_from_my_chi_api(config))
    return all_events

def extract_from_bibliocom_sources(config: Dict) -> List[Dict]:
    """Extract from Bibliocommons"""
    events_df = extract_helpers.final_bibliocom_df_fetch()
    events_df["source_type"] = config['source_type']
    return events_df.to_dict(orient="records")

def extract_libnet_source(config: Dict) -> List[Dict]:
    """
    Extracts from a single LibNet source using parameters from its config.
    """
    source_type = config['source_type']
    library_name = config['library_name']
    url = config.get('url')
    print(f"Extracting from LibNet source: {source_type} (library: '{library_name}', url: '{url or 'Default'}')")
    events_df = extract_helpers.final_libnet_df_fetch(library_name, url)
    events_df["source_type"] = source_type
    return events_df.to_dict(orient="records")

def extract_from_chicagoparent_web(config: Dict) -> List[Dict]:
    """Extract from ChicagoParent website"""
    events = extract_helpers.scrape_events_web(config.get('url'))
    return [{**event, "source_type": "chicagoparent"} for event in events]

def extract_from_mykidlist_web(config: Dict) -> List[Dict]:
    """Extract from mykidlist website"""
    events = extract_helpers.final_kidlist_df_fetch(days=config.get('days', 14))
    return [{**event, "source_type": "mykidlist"} for event in events]

def extract_from_hellobaby_web(config: Dict) -> List[Dict]:
    """Extract from hellobaby website"""
    events = extract_helpers.final_hellobaby_df_fetch()
    return [{**event, "source_type": "hellobaby"} for event in events]

def extract_drupal_source(config: Dict) -> List[Dict]:
    """Extracts from a single Drupal source using a generic function."""
    source_type = config['source_type']
    print(f"Extracting from Drupal source: {source_type}")
    events = extract_helpers.final_drupal_library_df_fetch(config)
    return [{**event, "source_type": source_type} for event in events]

# ==================== TRANSFORM ====================

def post_process_records(all_event_lists: List[List[Dict]], config: Dict) -> List[Dict]:
    """
    Combines, deduplicates, and applies final processing to transformed events.

    This function takes the output from all parallel transform tasks, combines them,
    runs cross-source deduplication, and applies price categorization.

    Args:
        all_event_lists: A list where each item is a list of transformed event 
                         dictionaries from a specific source/task.
        config: The full runtime configuration, needed for API keys etc.

    Returns:
        A final, clean list of event dictionaries ready for loading.
    """
    combined_events = [event for sublist in all_event_lists for event in sublist]
    if not combined_events:
        print("No events to post-process.")
        return []
    print(f"Combined {len(combined_events)} total transformed events for post-processing.")
    print(f"Starting deduplication...")
    
    # Convert dicts back to ProcessedEvent objects for the deduplication utility
    processed_events = [utils.dict_to_processed_event(e) for e in combined_events]
    
    deduped_events = utils.deduplicate_events(processed_events, fuzzy_threshold=90)
    print(f"After deduplication: {len(deduped_events)} events remain.")

    # 3. Add Price Categories (from old `post_process` task)
    final_events = []
    if deduped_events:
        print("Applying price categorization...")
        price_categorizer = PriceCategorizer(
            gemini_api_key=config.get("transform_config", {}).get('gemini_api_key')
        )
        categorized = price_categorizer.categorize_prices(deduped_events)
        # Convert final ProcessedEvent objects back to dicts for loading
        final_events = [utils.processed_event_to_dict(e) for e in categorized]

    print(f"Post-processing complete. {len(final_events)} final events ready for loading.")
    return final_events

def transform_records(raw_events: List[Dict], transform_config: Optional[Dict] = None) -> List[Dict]:
    """
    Transform raw event data into a standardized, serializable format.

    This function takes raw events, transforms them using the EventTransformer,
    and ensures the output is a list of dictionaries, ready for XCom.
    
    Args:
        raw_events: List of raw event dictionaries from extract phase.
        transform_config: Config for transformation (AI keys, mappings, etc.).
        
    Returns:
        List of transformed, serializable event dictionaries.
    """
    if not raw_events:
        print("No raw events to transform")
        return []
    
    # Initialize transformer
    transformer = EventTransformer(transform_config or {})
    
    # Group events by source for batch processing
    events_by_source = {}
    for event in raw_events:
        source = event.get('source_type', 'unknown')
        if source not in events_by_source:
            events_by_source[source] = []
        events_by_source[source].append(event)
    
    # Transform each source batch
    all_transformed_objects = []
    for source, source_events in events_by_source.items():
        print(f"Transforming {len(source_events)} events from {source}")
        transformed = transformer.transform_source_batch(source, source_events)
        all_transformed_objects.extend(transformed)
    
    # This section ensures the output is always a list of dicts, safe for Airflow's XCom.
    serializable_events = []
    for event in all_transformed_objects:
        if isinstance(event, dict):
            serializable_events.append(event)
        else:
            try:
                # Convert custom ProcessedEvent objects to dictionaries
                event_dict = utils.processed_event_to_dict(event)
                serializable_events.append(event_dict)
            except Exception as e:
                print(f"Error converting transformed event to dict: {e}")
                continue # Skip events that can't be serialized

    print(f"Transformation complete: {len(serializable_events)} transformed events ready.")
    return serializable_events

# ==================== LOAD ====================
def load_to_supabase(transformed_events: List[Dict], db_config: Dict) -> int:
    """
    Load transformed events to Supabase database using an upsert strategy.
    
    Args:
        transformed_events: List of transformed event dictionaries
        db_config: Database configuration (connection params, cleanup settings, etc.)
        
    Returns:
        Number of events successfully upserted
    """
    if not transformed_events:
        print("No events to load")
        return 0
    
    # Initialize database manager
    db_manager = DatabaseManager(
        db_url=db_config['connection_string'],
    )
    
    try:
        db_manager.connect()
        
        # Cleanup old events (older than one week) for specified sources
        sources_to_clean = list(set([event_dict.get("source_type") for event_dict in transformed_events]))
        if sources_to_clean:
            cutoff_date = datetime.utcnow().date() - timedelta(days=7)
            print(f"Cleaning up events from sources {sources_to_clean} older than {cutoff_date}")
            db_manager.cleanup_old_events_by_source(sources_to_clean, cutoff_date)
        
        # Convert dicts back to ProcessedEvent objects
        events = [utils.dict_to_processed_event(event_dict) for event_dict in transformed_events]
        
        # Create locations and categories
        print(f"Processing {len(events)} events for upsert...")
        db_manager.check_and_reconnect()
        
        loc_map = db_manager.find_or_create_locations([e.location_info for e in events])
        cat_names = {c for e in events for c in e.categories}
        cat_map = db_manager.find_or_create_categories(list(cat_names))
        
        # Bulk upsert events and get their DB IDs
        event_map = db_manager.bulk_upsert_events(events, loc_map)
        
        # Unlink existing categories for all upserted events before re-linking
        upserted_event_ids = list(event_map.values())
        if upserted_event_ids:
            print(f"Clearing old category links for {len(upserted_event_ids)} events.")
            db_manager.unlink_categories_for_events(upserted_event_ids)

        # Link events to their categories
        db_manager.link_events_to_categories(events, event_map, cat_map)
        
        upserted_count = len(event_map)
        print(f"Successfully upserted {upserted_count} events")
        return upserted_count
        
    except Exception as e:
        print(f"Error loading to database: {e}")
        raise
    finally:
        db_manager.disconnect()
