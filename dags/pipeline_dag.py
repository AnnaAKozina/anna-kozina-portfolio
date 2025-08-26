from __future__ import annotations
import pendulum
import json
from airflow.decorators import dag, task
from airflow.models import Variable
from typing import List, Dict, Any
from etl.pipeline import (
    extract_chicago_apis,
    extract_from_bibliocom_sources,
    extract_libnet_source,
    extract_drupal_source,
    extract_from_chicagoparent_web as extract_chicagoparent,
    extract_from_mykidlist_web as extract_mykidlist,
    extract_from_hellobaby_web as extract_hellobaby,
    post_process_records,
    transform_records,
    load_to_supabase
)

DEFAULT_ARGS = {"retries": 2}

@dag(
    schedule="0 3 * * *",  # every day at 03:00
    start_date=pendulum.datetime(2025, 8, 1, tz="America/Chicago"),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "supabase"],
)
def daily_etl_to_supabase_dynamic():
    
    @task
    def get_runtime_config() -> Dict[str, Any]:
        """Get the full pipeline configuration from Airflow Variables"""
        # This also groups source configs by type for easier processing
        cfg = json.loads(Variable.get("pipeline_config"))
        
        source_configs = cfg.get("extract_config", {})
        groups = {
            "chicago_apis": [],
            "bibliocom": [],
            "libnet": [],
            "drupal": [],
            "chicagoparent": None,
            "mykidlist": None,
            "hellobaby": None,
        }

        # --- Group configs by their type ---
        if source_configs.get("enable_pl_api"): groups["chicago_apis"].append({"source_type": "pl", **source_configs["pl_api_config"]})
        if source_configs.get("enable_pd_api"): groups["chicago_apis"].append({"source_type": "pd", **source_configs["pd_api_config"]})
        if source_configs.get("enable_my_chi_api"): groups["chicago_apis"].append({"source_type": "my_chi", **source_configs["my_chi_api_config"]})
        
        if source_configs.get("enable_evanston_scrape"): groups["bibliocom"].append({"source_type": "evanston_pl", **source_configs["evanston_config"]})
        
        if source_configs.get("enable_skokie_scrape"): groups["libnet"].append({"source_type": "skokie_pl", **source_configs["skokie_config"]})
        if source_configs.get("enable_wilmette_scrape"): groups["libnet"].append({"source_type": "wilmette_pl", **source_configs["wilmette_config"]})
        if source_configs.get("enable_mtprospect_scrape"): groups["libnet"].append({"source_type": "mtprospect_pl", **source_configs["mtprospect_config"]})
        if source_configs.get("enable_desplaines_scrape"): groups["libnet"].append({"source_type": "desplaines_pl", **source_configs["desplaines_config"]})
        if source_configs.get("enable_niles_scrape"): groups["libnet"].append({"source_type": "niles_pl", **source_configs["niles_config"]})
        if source_configs.get("enable_fremontlibrary_scrape"): groups["libnet"].append({"source_type": "fremontlibrary_pl", **source_configs["fremontlibrary_config"]})
        if source_configs.get("enable_indiantrails_scrape"): groups["libnet"].append({"source_type": "indiantrails_pl", **source_configs["indiantrails_config"]})
        if source_configs.get("enable_deerfield_scrape"): groups["libnet"].append({"source_type": "deerfield_pl", **source_configs["deerfield_config"]})
        if source_configs.get("enable_northbrook_scrape"): groups["libnet"].append({"source_type": "northbrook_pl", **source_configs["northbrook_config"]})

        if source_configs.get("enable_vernon_scrape"): groups["drupal"].append({"source_type": "vernon", **source_configs["vernon_config"]})
        if source_configs.get("enable_wauconda_scrape"): groups["drupal"].append({"source_type": "wauconda", **source_configs["wauconda_config"]})
        if source_configs.get("enable_winnetka_scrape"): groups["drupal"].append({"source_type": "winnetka", **source_configs["winnetka_config"]})

        if source_configs.get("enable_chicagoparent_scrape"): groups["chicagoparent"] = source_configs["chicagoparent_config"]
        if source_configs.get("enable_mykidlist_scrape"): groups["mykidlist"] = source_configs["mykidlist_config"]
        if source_configs.get("enable_hellobaby_scrape"): groups["hellobaby"] = source_configs["hellobaby_config"]

        cfg["grouped_source_configs"] = groups
        return cfg
    
    @task
    def get_source_group_config(config: dict, group_name: str) -> list:
        """
        Extracts a specific list of source configs from the main config dict.
        This provides a clean XCom output (a simple list) for dynamic mapping.
        """
        return config.get("grouped_source_configs", {}).get(group_name, [])
    
    @task
    def etl_chicago_apis(config: Dict) -> List[Dict]:
        raw_events = extract_chicago_apis(config["grouped_source_configs"]["chicago_apis"])
        return transform_records(raw_events, config["transform_config"])
    
    @task
    def etl_bibliocom_source(config: Dict, bibliocom_config: Dict) -> List[Dict]:
        raw_events = extract_from_bibliocom_sources(bibliocom_config)
        return transform_records(raw_events, config["transform_config"])

    @task
    def etl_libnet_source(config: Dict, libnet_config: Dict) -> List[Dict]:
        raw_events = extract_libnet_source(libnet_config)
        return transform_records(raw_events, config["transform_config"])

    @task
    def etl_drupal_source(config: Dict, drupal_config: Dict) -> List[Dict]:
        raw_events = extract_drupal_source(drupal_config)
        return transform_records(raw_events, config["transform_config"])

    @task
    def etl_chicagoparent(config: Dict) -> List[Dict]:
        raw_events = extract_chicagoparent(config["grouped_source_configs"]["chicagoparent"])
        return transform_records(raw_events, config["transform_config"])
        
    @task
    def etl_mykidlist(config: Dict) -> List[Dict]:
        raw_events = extract_mykidlist(config["grouped_source_configs"]["mykidlist"])
        return transform_records(raw_events, config["transform_config"])
    
    @task
    def etl_hellobaby(config: Dict) -> List[Dict]:
        raw_events = extract_hellobaby(config["grouped_source_configs"]["hellobaby"])
        return transform_records(raw_events, config["transform_config"])
    
    @task
    def combine_and_post_process(all_event_lists: list, config: Dict) -> List[Dict]:
        return post_process_records(all_event_lists, config)
    
    @task
    def load_data(rows: List[Dict], db_config: Dict) -> int:
        return load_to_supabase(rows, db_config)

    # ==================== DAG FLOW ====================
    
    config = get_runtime_config()
    
    # A list to hold the output of all our ETL tasks
    all_transformed_data = []

    # --- Run tasks for each group ---
    
    # 1. Chicago APIs (runs as a single task)
    chicago_data = etl_chicago_apis(config)
    all_transformed_data.append(chicago_data)

    # 2. BiblioCommons sources (mapped)
    bibliocom_configs = get_source_group_config(config, "bibliocom")
    bibliocom_data = etl_bibliocom_source.partial(config=config).expand(
        bibliocom_config=bibliocom_configs
    )
    all_transformed_data.append(bibliocom_data)

    # 3. LibNet sources (mapped)
    libnet_configs = get_source_group_config(config, "libnet")
    libnet_data = etl_libnet_source.partial(config=config).expand(
        libnet_config=libnet_configs
    )
    all_transformed_data.append(libnet_data)

    # 4. Drupal sources (mapped)
    drupal_configs = get_source_group_config(config, "drupal")
    drupal_data = etl_drupal_source.partial(config=config).expand(
        drupal_config=drupal_configs
    )
    all_transformed_data.append(drupal_data)

    # 5. Other sources (non-mapped)
    chicagoparent_data = etl_chicagoparent(config)
    mykidlist_data = etl_mykidlist(config)
    hellobaby_data = etl_hellobaby(config)
    all_transformed_data.extend([chicagoparent_data, mykidlist_data, hellobaby_data])

    # Combine, Post-Process, and Load
    final_data = combine_and_post_process(all_transformed_data, config)
    load_data(final_data, config["db_config"])

dag = daily_etl_to_supabase_dynamic()
