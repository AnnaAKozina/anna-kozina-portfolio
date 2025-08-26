import google.generativeai as genai
from etl import utils, location, category, age

import pandas as pd
import re, json, time
from datetime import date
from typing import List, Dict, Tuple, Optional

class EventTransformer:
    """Handles the transformation logic extracted from EventPipeline"""
    
    def __init__(self, config: Dict):
        self.config = config
        
        # Initialize AI components if available
        gemini_key = config.get('gemini_api_key') or config.get('GEMINI_API_KEY')
        if gemini_key:
            genai.configure(api_key=gemini_key)
            self.gemini_model = genai.GenerativeModel("gemini-2.5-flash-lite")
        else:
            self.gemini_model = None
            print("Warning: GEMINI_API_KEY not found. AI features disabled.")
        
        # Initialize helpers
        self.location_resolver = location.LocationResolver(self.config)
        self.age_extractor = age.AgeExtractor(self.gemini_model)
        self.category_mapper = None
        self._cached_categories = config.get('target_categories') or utils.FALLBACK_CATEGORIES
    
    def transform_source_batch(self, source_type: str, raw_events: List[Dict]) -> List[Dict]:
        """Transform a batch of events from a single source"""
        
        if source_type == "chicagoparent":
            return self._transform_chicagoparent_events(raw_events)
        else:
            # Convert to DataFrame for existing logic
            df = pd.DataFrame(raw_events)
            return self._transform_dataframe_events(df, source_type)
    
    def _transform_chicagoparent_events(self, raw_events: List[Dict]) -> List[Dict]:
        """Transform ChicagoParent events using AI"""
        if not self.gemini_model:
            print("Cannot transform ChicagoParent events: Gemini AI not available")
            return []
        
        transformed_events = []
        
        for idx, raw_event in enumerate(raw_events, start=1):
            title = raw_event.get("title", "Unknown")
            print(f"  > Processing event {idx}/{len(raw_events)}: {title}")
            
            try:
                prompt = utils.create_chicagoparent_prompt(raw_event, self._cached_categories)
                response_text = self.gemini_model.generate_content(prompt).text
                time.sleep(4)  # Rate limiting
                
                # Extract JSON from AI response
                match = re.search(r"\{.*\}", response_text, re.DOTALL)
                if not match:
                    print("    - Skipping: No JSON found in AI response.")
                    continue
                
                parsed_data = json.loads(match.group(0))
                
                # Resolve location
                address_str = " ".join(filter(None, [
                    parsed_data.get("location_address"),
                    parsed_data.get("location_city"), 
                    parsed_data.get("location_state"),
                    parsed_data.get("location_zip")
                ]))
                resolved_location = self.location_resolver.resolve(
                    name=parsed_data.get("location_name"),
                    address=address_str
                )
                if not resolved_location:
                    print("    - Skipping: Location could not be resolved.")
                    continue
                
                # Handle multiple event dates
                today_str = date.today().strftime("%Y-%m-%d")
                event_dates = [d for d in (parsed_data.get("event_date") or [None]) if d and d >= today_str]
                
                for event_date in event_dates:
                    event_dict = {
                        "name": parsed_data.get("name"),
                        "source_type": "chicagoparent", 
                        "source_id": f"cp_{raw_event.get('url', '')}_{event_date or 'nodate'}",
                        "description": parsed_data.get("description"),
                        "event_date": event_date,
                        "event_date_comment": parsed_data.get("event_date_comment"),
                        "start_time": parsed_data.get("start_time"),
                        "end_time": parsed_data.get("end_time"),
                        "url": parsed_data.get("url"),
                        "min_age": parsed_data.get("min_age"),
                        "max_age": parsed_data.get("max_age"),
                        "price": str(parsed_data.get("price", "Free")),
                        "registration_needed": parsed_data.get("registration_needed", False),
                        "registration_closed": parsed_data.get("registration_closed", False),
                        "categories": [c for c in parsed_data.get("categories", []) if c in self._cached_categories],
                        "location_info": resolved_location.__dict__
                    }
                    transformed_events.append(event_dict)
                    
            except Exception as e:
                print(f"    - Error processing event: {e}")
                continue
        
        return transformed_events
    
    def _transform_dataframe_events(self, df: pd.DataFrame, source_type: str) -> List[Dict]:
        """Transform DataFrame-based events"""
        
        # Get the appropriate mapping for this source
        mapping = self._get_mapping_for_source(source_type)
        if not mapping:
            print(f"No mapping found for source: {source_type}")
            return []
        
        # Generate AI mappings
        category_map = self._get_ai_category_mapping(df, mapping)
        age_map = self._get_ai_age_mapping(df, mapping)
        print("age_map", age_map)
        
        # Process DataFrame
        return self._process_dataframe_to_dicts(df, mapping, category_map, age_map)
    
    def _get_mapping_for_source(self, source_type: str) -> Optional[Dict]:
        """Get the field mapping configuration for a source type"""
        mapping_lookup = {
            "pl": utils.PL_MAPPING,
            "pd": utils.PD_MAPPING, 
            "my_chi": utils.MY_CHI_MAPPING,
            "evanston_pl": utils.EVANSTON_PL_MAPPING,
            "skokie_pl": {**utils.LIBNET_PL_MAPPING, "source_type": "skokie_pl"},
            "wilmette_pl": {**utils.LIBNET_PL_MAPPING, "source_type": "wilmette_pl"},
            "mtprospect_pl": {**utils.LIBNET_PL_MAPPING, "source_type": "mtprospect_pl"},
            "desplaines_pl": {**utils.LIBNET_PL_MAPPING, "source_type": "desplaines_pl"},
            "niles_pl": {**utils.LIBNET_PL_MAPPING, "source_type": "niles_pl"},
            "fremontlibrary_pl": {**utils.LIBNET_PL_MAPPING, "source_type": "fremontlibrary_pl"},
            "deerfield_pl": {**utils.LIBNET_PL_MAPPING, "source_type": "deerfield_pl"},
            "northbrook_pl": {**utils.LIBNET_PL_MAPPING, "source_type": "northbrook_pl"},
            "indiantrails_pl": {**utils.LIBNET_PL_MAPPING, "source_type": "indiantrails_pl"},
            "mykidlist": {**utils.MYKIDLIST_MAPPING, "source_type": "mykidlist"},
            "hellobaby": {**utils.HELLOBABY_MAPPING, "source_type": "hellobaby"},
            "vernon": {**utils.DRUPAL_PL_MAPPING, "source_type": "vernon"},
            "wauconda": {**utils.DRUPAL_PL_MAPPING, "source_type": "wauconda"},
            "winnetka":  {**utils.DRUPAL_PL_MAPPING, "source_type": "winnetka"},
        }
        return mapping_lookup.get(source_type)
    
    def _get_ai_category_mapping(self, df: pd.DataFrame, mapping: Dict) -> Dict[str, List[str]]:
        """Generate AI category mapping for a single DataFrame"""
        if not self.gemini_model:
            return {}
        
        self.category_mapper = category.CategoryMapper(self._cached_categories, self.gemini_model)
        
        # Extract unique category values
        cat_col = mapping.get("category_col")
        raw_cats = set()
        if cat_col and cat_col in df.columns:
            vals = df[cat_col].dropna().astype(str).str.split("|").explode().str.strip()
            raw_cats.update(vals.unique())
        raw_cats.add("")  # for default mapping
        
        return self.category_mapper.generate_mapping(sorted(raw_cats))
    
    def _get_ai_age_mapping(self, df: pd.DataFrame, mapping: Dict) -> Dict[str, Tuple[int, int]]:
        """Generate AI age mapping for a single DataFrame"""
        if not self.gemini_model:
            return {}
        
        # Extract unique age values
        age_col = mapping.get("age_col")
        raw_ages = set()
        if age_col and age_col in df.columns:
            vals = df[age_col].dropna().astype(str).str.split("|").explode().str.strip()
            raw_ages.update(vals.unique())
        return self.age_extractor.get_mapping_from_model(sorted(raw_ages))
    
    def _process_dataframe_to_dicts(
        self, 
        df: pd.DataFrame, 
        mapping: Dict, 
        category_map: Dict, 
        age_map: Dict
    ) -> List[Dict]:
        """Process DataFrame rows into event dictionaries"""
        
        events = []
        src_type = mapping["source_type"]
        df = df.fillna("")

        def is_empty(value):
            """Check if value is empty (handles None, '', np.nan, empty collections)"""
            if value is None:
                return True
            if pd.isna(value):  # Handles np.nan, pd.NaT, pd.NA
                return True
            if hasattr(value, '__len__') and len(value) == 0:  # Empty strings, lists, etc.
                return True
            return False
        
        for _, row in df.iterrows():
            try:
                # Skip cancelled events
                if row.get(mapping["cancelled_col"]) == mapping.get("cancelled_value", True):
                    continue

                # Skip events without url
                if is_empty(row.get(mapping["url_col"])):
                    continue

                # Age filter
                min_age, max_age = self._parse_age_from_row(row, mapping, age_map)
                if not utils.is_for_children(min_age, max_age):
                    continue
                
                # Location
                resolved_loc = self.location_resolver.resolve(
                    name=row.get(mapping["loc_name_col"]),
                    address=row.get(mapping["loc_address_col"]),
                    latitude=pd.to_numeric(row.get(mapping.get("lat_col")), errors="coerce") if row.get(mapping.get("lat_col")) else None,
                    longitude=pd.to_numeric(row.get(mapping.get("lng_col")), errors="coerce") if row.get(mapping.get("lng_col")) else None,
                )
                if not resolved_loc or pd.isna(resolved_loc.latitude):
                    continue
                
                # Categories
                categories = self._map_categories(
                    row.get(mapping.get("category_col", "")), category_map
                )
                
                # Dates & Times
                start_dt = utils.standardize_date(row.get(mapping["start_date_col"]))
                start_time = self._parse_time_from_row(row, mapping, "start")
                end_time = self._parse_time_from_row(row, mapping, "end")
                
                # Registration
                reg_status = str(row.get(mapping.get("reg_status_col", ""))).strip().upper()
                reg_needed = bool(row.get(mapping.get("reg_link_col"))) or reg_status not in {"NOT REQUIRED", "", "NONE"}
                reg_closed = utils.registration_closed(
                    reg_str=row.get(mapping.get("reg_open_col", "")),
                    reg_deadline=row.get(mapping.get("reg_ends_col", "")),
                )
                
                # Price & URL
                price = str(row.get(mapping.get("price_col")) or mapping.get("price_val", "Free"))
                url = utils.extract_first_url(str(row.get(mapping["url_col"])))
                
                event_dict = {
                    "name": str(row.get(mapping["name_col"])),
                    "source_type": src_type,
                    "source_id": str(row.get(mapping["source_id_col"])),
                    "description": str(row.get(mapping["desc_col"])),
                    "event_date": start_dt.strftime("%Y-%m-%d") if start_dt else None,
                    "event_date_comment": None,
                    "start_time": start_time.strftime("%H:%M") if start_time else None,
                    "end_time": end_time.strftime("%H:%M") if end_time else None,
                    "url": url,
                    "min_age": min_age,
                    "max_age": max_age,
                    "price": price,
                    "registration_needed": reg_needed,
                    "registration_closed": reg_closed,
                    "categories": categories,
                    "location_info": resolved_loc.__dict__,
                }
                events.append(event_dict)
                
            except Exception as e:
                print(f"Error processing row for '{src_type}': {e}")
                continue
        
        return events
    
    def _parse_age_from_row(self, row, mapping, age_map):
        """Parse age information from DataFrame row"""
        if "age_col" in mapping:
            return self.age_extractor.extract_age_for_event(
                row.get(mapping["age_col"]), age_map
            )
        elif "min_age_col" in mapping and "max_age_col" in mapping:
            return (
                int(row.get(mapping["min_age_col"], 0) or 0),
                int(row.get(mapping["max_age_col"], 0) or 0),
            )
        return None, None
    
    def _parse_time_from_row(self, row, mapping, ttype):
        """Parse time information from DataFrame row"""
        col = mapping.get(f"{ttype}_time_col")
        if col and row.get(col):
            return utils.extract_time(row.get(col))
        date_col = mapping.get(f"{ttype}_date_col")
        return utils.extract_time(row.get(date_col)) if date_col else None
    
    def _map_categories(self, cat_name, cat_map):
        """Map category names using AI mapping"""
        default = ["General Family Activities"]
        if not cat_name:
            return cat_map.get("", default)
        if "|" in str(cat_name):
            final = set()
            for part in str(cat_name).split("|"):
                final.update(cat_map.get(part.strip(), default))
            return list(final)
        return cat_map.get(cat_name, default)
