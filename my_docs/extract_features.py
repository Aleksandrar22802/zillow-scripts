#!/usr/bin/env python3
"""
Feature extraction from GraphQL and RapidAPI Zillow property JSON files.
Extracts features defined in my_docs/feature_order.md from both directories.
"""

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional, List, Union
import csv


US_COUNTRY_VALUES = {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}

US_STATE_NAME_TO_CODE = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC",
}

US_STATE_CODES = set(US_STATE_NAME_TO_CODE.values())

EXCLUDED_STATE_CODES = {
    "AK",  # Alaska
    "ID",  # Idaho
    "KS",  # Kansas
    "LA",  # Louisiana
    "MS",  # Mississippi
    "MO",  # Missouri
    "MT",  # Montana
    "NM",  # New Mexico
    "ND",  # North Dakota
    "TX",  # Texas
    "UT",  # Utah
    "WY",  # Wyoming
}


def _normalize_state_code(raw_state: Optional[str]) -> Optional[str]:
    if not raw_state:
        return None

    state = str(raw_state).strip().upper()
    if not state:
        return None

    if state in US_STATE_CODES:
        return state

    return US_STATE_NAME_TO_CODE.get(state)


def _extract_country_and_state(data: Dict[str, Any]) -> tuple[Optional[str], Optional[str]]:
    address = data.get("address") if isinstance(data.get("address"), dict) else {}

    country = (
        data.get("country")
        or address.get("country")
        or data.get("countryName")
        or address.get("countryName")
    )
    if country is not None:
        country = str(country).strip().upper() or None

    state = data.get("state") or address.get("state")
    state_code = _normalize_state_code(state)

    return country, state_code


def _passes_location_filter(data: Dict[str, Any]) -> bool:
    country, state_code = _extract_country_and_state(data)

    # If country is present and not US, reject.
    if country and country not in US_COUNTRY_VALUES:
        return False

    # If country is missing, infer US only when state can be mapped to a US state code.
    if not country and not state_code:
        return False

    # Reject when state is missing (cannot apply excluded-state policy reliably).
    if not state_code:
        return False

    # Enforce excluded-state policy.
    if state_code in EXCLUDED_STATE_CODES:
        return False

    return True


class FeatureExtractor:
    """
    Extracts 79 property features from Zillow GraphQL and RapidAPI JSON data.
    
    This class provides methods to extract various property attributes including:
    - Physical characteristics (size, age, stories, parking)
    - Amenities and features (pools, garages, fireplaces, etc.)
    - School information (counts, ratings, distances)
    - Market data (sale prices, appreciation rates)
    - Boolean flags for property conditions
    - One-hot encoded home types
    
    All features are mapped to their corresponding JSON field names and normalized
    to handle missing data gracefully (returns None or 0 as appropriate).
    """
    
    # Define feature extraction rules (79 total features from feature_order.md)
    # These are extracted in the exact order specified in my_docs/feature_order.md
    FEATURES = [
        'living_area_sqft',
        'lot_size_sqft',
        'bedrooms',
        'bathrooms',
        'latitude',
        'longitude',
        'year_built',
        'property_age',
        'stories',
        'bathrooms_full',
        'bathrooms_half',
        'bathrooms_three_quarter',
        'garage_capacity',
        'covered_parking_capacity',
        'open_parking_capacity',
        'parking_capacity_total',
        'fireplaces_count',
        'cooling_count',
        'heating_count',
        'appliances_count',
        'flooring_count',
        'construction_materials_count',
        'interior_features_count',
        'exterior_features_count',
        'community_features_count',
        'parking_features_count',
        'pool_features_count',
        'laundry_features_count',
        'lot_features_count',
        'view_features_count',
        'sewer_count',
        'water_source_count',
        'electric_count',
        'school_count',
        'min_school_distance',
        'avg_school_rating',
        'max_school_rating',
        'elementary_rating',
        'middle_rating',
        'high_rating',
        'total_parking',
        'total_bathrooms',
        'lot_to_living_ratio',
        'beds_per_bath',
        'total_amenity_count',
        'previous_sale_price',
        'years_since_last_sale',
        'months_since_last_sale',
        'price_change_since_last_sale',
        'price_appreciation_rate',
        'annual_appreciation_rate',
        'has_basement',
        'has_garage',
        'has_attached_garage',
        'has_cooling',
        'has_heating',
        'has_fireplace',
        'has_spa',
        'has_view',
        'has_pool',
        'has_open_parking',
        'has_home_warranty',
        'is_new_construction',
        'is_senior_community',
        'has_waterfront_view',
        'has_central_air',
        'has_forced_air_heating',
        'has_natural_gas',
        'has_hardwood_floors',
        'has_tile_floors',
        'has_any_pool_or_spa',
        'has_previous_sale_data',
        'is_recent_flip',
        'home_type_SINGLE_FAMILY',
        'home_type_MULTI_FAMILY',
        'home_type_MANUFACTURED',
        'home_type_LOT',
        'home_type_HOME_TYPE_UNKNOWN',
        'home_type_nan',
    ]
    
    def __init__(self, data: Dict[str, Any]):
        """
        Initialize extractor with property JSON data.
        
        Args:
            data: Complete property JSON object from Zillow GraphQL or RapidAPI
        
        Stores references to commonly-accessed nested objects for efficient extraction.
        """
        # Root property data object
        self.data = data
        
        # Nested object: detailed property facts and features (camelCase keys from API)
        self.reso_facts = data.get('resoFacts', {})
        
        # Array of historical price/event records (e.g., "Sold", "Listed for sale")
        self.price_history = data.get('priceHistory', [])
        
        # Array of historical tax assessment records
        self.tax_history = data.get('taxHistory', [])
        
        # Array of nearby schools with names, ratings, distances
        self.schools = data.get('schools', [])
        
    def extract_all(self) -> Dict[str, Any]:
        """
        Extract all 79 features from the property data.
        
        Returns:
            Dictionary mapping feature names to extracted values.
            Missing/invalid features are set to None.
        
        This method dynamically calls each _extract_*() method using reflection.
        If a method doesn't exist or raises an exception, the feature defaults to None.
        """
        # Initialize empty dict to hold extracted features
        features = {}
        
        # Iterate through all 79 feature names in order
        for feature in self.FEATURES:
            try:
                # Dynamically call the corresponding extraction method
                # Method names follow pattern: _extract_<feature_name>
                extraction_method = getattr(self, f'_extract_{feature}', lambda: None)
                features[feature] = extraction_method()
            except Exception as e:
                # On any error, set feature to None (graceful degradation)
                features[feature] = None
        
        return features
    
    # Size features
    def _extract_living_area_sqft(self) -> Optional[float]:
        return self.data.get('livingArea')
    
    def _extract_lot_size_sqft(self) -> Optional[float]:
        return self.data.get('lotSize')
    
    def _extract_bedrooms(self) -> Optional[int]:
        return self.data.get('bedrooms')
    
    def _extract_bathrooms(self) -> Optional[float]:
        return self.data.get('bathrooms')
    
    def _extract_total_bathrooms(self) -> Optional[float]:
        return self.data.get('bathrooms')
    
    # Location
    def _extract_latitude(self) -> Optional[float]:
        return self.data.get('latitude')
    
    def _extract_longitude(self) -> Optional[float]:
        return self.data.get('longitude')
    
    def _extract_year_built(self) -> Optional[int]:
        return self.data.get('yearBuilt')
    
    def _extract_property_age(self) -> Optional[int]:
        year_built = self.data.get('yearBuilt')
        if year_built:
            return datetime.now().year - year_built
        return None
    
    # Structural
    def _extract_stories(self) -> Optional[int]:
        return self.reso_facts.get('stories')
    
    def _extract_bathrooms_full(self) -> Optional[int]:
        return self.reso_facts.get('bathroomsFull')
    
    def _extract_bathrooms_half(self) -> Optional[int]:
        return self.reso_facts.get('bathroomsHalf')
    
    def _extract_bathrooms_three_quarter(self) -> Optional[int]:
        # May not be in standard schema
        return self.reso_facts.get('bathroomsThreeQuarter')
    
    # Parking
    def _extract_garage_capacity(self) -> Optional[int]:
        return self.reso_facts.get('garageParkingCapacity')
    
    def _extract_covered_parking_capacity(self) -> Optional[int]:
        return self.reso_facts.get('coveredParkingCapacity')
    
    def _extract_open_parking_capacity(self) -> Optional[int]:
        return self.reso_facts.get('openParkingCapacity')
    
    def _extract_parking_capacity_total(self) -> Optional[int]:
        return self.reso_facts.get('parkingCapacity')
    
    def _extract_total_parking(self) -> Optional[int]:
        return self.reso_facts.get('parkingCapacity')
    
    # Amenities - counts
    def _extract_fireplaces_count(self) -> Optional[int]:
        val = self.reso_facts.get('fireplaces')
        if isinstance(val, (int, float)):
            return int(val) if val > 0 else 0
        # Could be a string or list
        # ========================================================================
        # SIZE FEATURES (4 total)
        # ========================================================================
        # Basic property dimensions: living area, lot size, bedroom/bathroom counts
    
        def _extract_living_area_sqft(self) -> Optional[float]:
            """Extract interior living area in square feet."""
            return self.data.get('livingArea')
    
        def _extract_lot_size_sqft(self) -> Optional[float]:
            """Extract total lot size in square feet."""
            return self.data.get('lotSize')
    
        def _extract_bedrooms(self) -> Optional[int]:
            """Extract number of bedrooms."""
            return self.data.get('bedrooms')
    
        def _extract_bathrooms(self) -> Optional[float]:
            """Extract number of bathrooms (may include half-baths as 0.5)."""
            return self.data.get('bathrooms')
    
        def _extract_total_bathrooms(self) -> Optional[float]:
            """Duplicate of bathrooms for consistency in feature list."""
            return self.data.get('bathrooms')
    
        # ========================================================================
        # LOCATION FEATURES (4 total)
        # ========================================================================
        # Geographic coordinates and construction year
    
        def _extract_latitude(self) -> Optional[float]:
            """Extract latitude coordinate of property."""
            return self.data.get('latitude')
    
        def _extract_longitude(self) -> Optional[float]:
            """Extract longitude coordinate of property."""
            return self.data.get('longitude')
    
        def _extract_year_built(self) -> Optional[int]:
            """Extract year the property was built."""
            return self.data.get('yearBuilt')
    
        def _extract_property_age(self) -> Optional[int]:
            """Calculate property age in years from yearBuilt.
        
            Returns:
                Years since construction (current_year - year_built).
                Returns None if yearBuilt is missing.
            """
            year_built = self.data.get('yearBuilt')
            if year_built:
                return datetime.now().year - year_built
            return None
    
        # ========================================================================
        # STRUCTURAL FEATURES (4 total)
        # ========================================================================
        # Building structure details: stories, bathroom breakdowns
    
        def _extract_stories(self) -> Optional[int]:
            """Extract number of stories/floors in the property."""
            return self.reso_facts.get('stories')
    
        def _extract_bathrooms_full(self) -> Optional[int]:
            """Extract count of full bathrooms (have tub/shower and toilet)."""
            return self.reso_facts.get('bathroomsFull')
    
        def _extract_bathrooms_half(self) -> Optional[int]:
            """Extract count of half-bathrooms (have toilet and sink only)."""
            return self.reso_facts.get('bathroomsHalf')
    
        def _extract_bathrooms_three_quarter(self) -> Optional[int]:
            """Extract count of 3/4 bathrooms (have toilet, sink, and shower only).
        
            Note: May not always be present in the API schema.
            """
            return self.reso_facts.get('bathroomsThreeQuarter')
    
        # ========================================================================
        # PARKING FEATURES (5 total)
        # ========================================================================
        # Parking capacity: garage, covered, open, and total spaces
    
        def _extract_garage_capacity(self) -> Optional[int]:
            """Extract number of garage parking spaces."""
            return self.reso_facts.get('garageParkingCapacity')
    
        def _extract_covered_parking_capacity(self) -> Optional[int]:
            """Extract number of covered parking spaces (carport style)."""
            return self.reso_facts.get('coveredParkingCapacity')
    
        def _extract_open_parking_capacity(self) -> Optional[int]:
            """Extract number of open/uncovered parking spaces."""
            return self.reso_facts.get('openParkingCapacity')
    
        def _extract_parking_capacity_total(self) -> Optional[int]:
            """Extract total parking capacity (may be sum of all types)."""
            return self.reso_facts.get('parkingCapacity')
    
        def _extract_total_parking(self) -> Optional[int]:
            """Duplicate of parking_capacity_total for consistency in feature list."""
            return self.reso_facts.get('parkingCapacity')
        return 1 if val else 0
    
    def _extract_electric_count(self) -> Optional[int]:
        return self._count_features('electric')
    
    # Schools
    def _extract_school_count(self) -> Optional[int]:
        return len(self.schools)
    
    def _extract_min_school_distance(self) -> Optional[float]:
        distances = [s.get('distance') for s in self.schools if s.get('distance') is not None]
        return min(distances) if distances else None
    
    def _extract_avg_school_rating(self) -> Optional[float]:
        ratings = [s.get('rating') for s in self.schools if s.get('rating') is not None]
        if ratings:
            return sum(ratings) / len(ratings)
        return None
    
    def _extract_max_school_rating(self) -> Optional[float]:
        ratings = [s.get('rating') for s in self.schools if s.get('rating') is not None]
        return max(ratings) if ratings else None
    
    def _extract_school_rating_by_type(self, school_type: str) -> Optional[float]:
        """Extract rating for elementary/middle/high school."""
        for school in self.schools:
            grades = school.get('grades', '')
            if school_type.lower() in grades.lower():
                return school.get('rating')
        return None
    
    def _extract_elementary_rating(self) -> Optional[float]:
        return self._extract_school_rating_by_type('elementary')
    
    def _extract_middle_rating(self) -> Optional[float]:
        return self._extract_school_rating_by_type('middle')
    
    def _extract_high_rating(self) -> Optional[float]:
        return self._extract_school_rating_by_type('high')
    
    # Ratios & derived
    def _extract_lot_to_living_ratio(self) -> Optional[float]:
        living = self.data.get('livingArea')
        lot = self.data.get('lotSize')
        if living and lot and living > 0:
            return lot / living
        return None
    
    def _extract_beds_per_bath(self) -> Optional[float]:
        beds = self.data.get('bedrooms')
        baths = self.data.get('bathrooms')
        if beds and baths and baths > 0:
            return beds / baths
        return None
    
    def _extract_total_amenity_count(self) -> Optional[int]:
        """Sum of all feature counts."""
        counts = [
            self._count_features('interiorFeatures'),
            self._count_features('exteriorFeatures'),
            self._count_features('poolFeatures'),
            self._count_features('heating'),
            self._count_features('cooling'),
        ]
        return sum([c for c in counts if c is not None])
    
    # Sale history
    def _extract_previous_sale_price(self) -> Optional[int]:
        """Get previous sale price from price history (before current)."""
        sold_items = [item for item in self.price_history if item.get('event') == 'Sold']
        if len(sold_items) >= 2:
            return sold_items[-2].get('price')
        return None
    
    def _extract_years_since_last_sale(self) -> Optional[int]:
        """Calculate years since last sale."""
        for item in reversed(self.price_history):
            if item.get('event') == 'Sold':
                date_str = item.get('date')
                if date_str:
                    try:
                        sale_date = datetime.strptime(date_str, '%Y-%m-%d')
                        return (datetime.now() - sale_date).days // 365
                    except (ValueError, TypeError):
                        pass
        return None
    
    def _extract_months_since_last_sale(self) -> Optional[int]:
        """Calculate months since last sale."""
        for item in reversed(self.price_history):
            if item.get('event') == 'Sold':
                date_str = item.get('date')
                if date_str:
                    try:
                        sale_date = datetime.strptime(date_str, '%Y-%m-%d')
                        return (datetime.now() - sale_date).days // 30
                    except (ValueError, TypeError):
                        pass
        return None
    
    def _extract_price_change_since_last_sale(self) -> Optional[int]:
        """Price change from previous sale to current."""
        current_price = self.data.get('price')
        previous_price = self._extract_previous_sale_price()
        if current_price and previous_price:
            return current_price - previous_price
        return None
    
    def _extract_price_appreciation_rate(self) -> Optional[float]:
        """Calculate price appreciation rate."""
        current_price = self.data.get('price')
        previous_price = self._extract_previous_sale_price()
        if current_price and previous_price and previous_price > 0:
            return (current_price - previous_price) / previous_price
        return None
    
    def _extract_annual_appreciation_rate(self) -> Optional[float]:
        """Annualized appreciation rate."""
        years = self._extract_years_since_last_sale()
        pct_rate = self._extract_price_appreciation_rate()
        if pct_rate and years and years > 0:
            return pct_rate / years
        return None
    
    # Boolean features
    def _extract_has_basement(self) -> Optional[int]:
        return 1 if self.reso_facts.get('basement') else 0
    
    def _extract_has_garage(self) -> Optional[int]:
        garage_capacity = self.reso_facts.get('garageParkingCapacity')
        return 1 if garage_capacity and garage_capacity > 0 else 0
    
    def _extract_has_attached_garage(self) -> Optional[int]:
        return 1 if self.reso_facts.get('attachedGarage') else 0
    
    def _extract_has_cooling(self) -> Optional[int]:
        cooling = self.reso_facts.get('cooling')
        return 1 if cooling else 0
    
    def _extract_has_heating(self) -> Optional[int]:
        heating = self.reso_facts.get('heating')
        return 1 if heating else 0
    
    def _extract_has_fireplace(self) -> Optional[int]:
        fireplaces = self.reso_facts.get('fireplaces')
        return 1 if fireplaces else 0
    
    def _extract_has_spa(self) -> Optional[int]:
        pool_features = self.reso_facts.get('poolFeatures')
        if pool_features and isinstance(pool_features, str):
            return 1 if 'spa' in pool_features.lower() else 0
        return 0
    
    def _extract_has_view(self) -> Optional[int]:
        view_features = self.reso_facts.get('viewFeatures')
        return 1 if view_features else 0
    
    def _extract_has_pool(self) -> Optional[int]:
        pool_features = self.reso_facts.get('poolFeatures')
        if pool_features and isinstance(pool_features, str):
            return 1 if 'pool' in pool_features.lower() else 0
        return 0
    
    def _extract_has_open_parking(self) -> Optional[int]:
        return 1 if self.reso_facts.get('openParkingCapacity') else 0
    
    def _extract_has_home_warranty(self) -> Optional[int]:
        return 1 if self.reso_facts.get('homeWarranty') else 0
    
    def _extract_is_new_construction(self) -> Optional[int]:
        return 1 if self.reso_facts.get('newConstruction') else 0
    
    def _extract_is_senior_community(self) -> Optional[int]:
        return 1 if self.reso_facts.get('seniorCommunity') else 0
    
    def _extract_has_waterfront_view(self) -> Optional[int]:
        view_features = self.reso_facts.get('viewFeatures')
        if view_features and isinstance(view_features, str):
            return 1 if 'water' in view_features.lower() else 0
        return 0
    
    def _extract_has_central_air(self) -> Optional[int]:
        cooling = self.reso_facts.get('cooling')
        if cooling and isinstance(cooling, str):
            return 1 if 'central' in cooling.lower() else 0
        return 0
    
    def _extract_has_forced_air_heating(self) -> Optional[int]:
        heating = self.reso_facts.get('heating')
        if heating and isinstance(heating, str):
            return 1 if 'forced air' in heating.lower() else 0
        return 0
    
    def _extract_has_natural_gas(self) -> Optional[int]:
        return 1 if self.reso_facts.get('naturalGas') else 0
    
    def _extract_has_hardwood_floors(self) -> Optional[int]:
        flooring = self.reso_facts.get('flooring')
        if flooring and isinstance(flooring, str):
            return 1 if 'hardwood' in flooring.lower() else 0
        return 0
    
    def _extract_has_tile_floors(self) -> Optional[int]:
        flooring = self.reso_facts.get('flooring')
        if flooring and isinstance(flooring, str):
            return 1 if 'tile' in flooring.lower() else 0
        return 0
    
    def _extract_has_any_pool_or_spa(self) -> Optional[int]:
        has_pool = self._extract_has_pool()
        has_spa = self._extract_has_spa()
        return 1 if (has_pool == 1 or has_spa == 1) else 0
    
    def _extract_has_previous_sale_data(self) -> Optional[int]:
        return 1 if any(item.get('event') == 'Sold' for item in self.price_history) else 0
    
    def _extract_is_recent_flip(self) -> Optional[int]:
        """Detect recent flip: purchased and resold within 2 years."""
        years = self._extract_years_since_last_sale()
        return 1 if years and years <= 2 else 0
    
    # Home type one-hot encoding
    def _extract_home_type(self) -> Optional[str]:
        return self.data.get('homeType')
    
    def _extract_home_type_SINGLE_FAMILY(self) -> Optional[int]:
        return 1 if self._extract_home_type() == 'SINGLE_FAMILY' else 0
    
    def _extract_home_type_MULTI_FAMILY(self) -> Optional[int]:
        return 1 if self._extract_home_type() == 'MULTI_FAMILY' else 0
    
    def _extract_home_type_MANUFACTURED(self) -> Optional[int]:
        return 1 if self._extract_home_type() == 'MANUFACTURED' else 0
    
    def _extract_home_type_LOT(self) -> Optional[int]:
        return 1 if self._extract_home_type() == 'LOT' else 0
    
    def _extract_home_type_HOME_TYPE_UNKNOWN(self) -> Optional[int]:
        return 1 if self._extract_home_type() == 'HOME_TYPE_UNKNOWN' else 0
    
    def _extract_home_type_nan(self) -> Optional[int]:
        home_type = self._extract_home_type()
        return 1 if home_type is None or home_type == '' else 0


def process_directory(directory_path: Path, output_csv: Path) -> None:
    """Process all JSON files in a directory and export features to CSV."""
    json_files = sorted(directory_path.glob('**/*.json'))
    
    if not json_files:
        print(f"No JSON files found in {directory_path}")
        return
    
    print(f"Processing {len(json_files)} files from {directory_path}")
    
    all_features = []
    skipped_location = 0
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not _passes_location_filter(data):
                skipped_location += 1
                print(f"  - {json_file.name}: skipped by location filter")
                continue
            
            extractor = FeatureExtractor(data)
            features = extractor.extract_all()
            features['zpid'] = data.get('zpid')
            features['_fetchMethod'] = data.get('_fetchMethod', 'unknown')
            features['_zipcodeHint'] = data.get('_zipcodeHint', 'unknown')
            all_features.append(features)
            
            print(f"  ✓ {json_file.name}")
        except Exception as e:
            print(f"  ✗ {json_file.name}: {e}")
    
    # Write to CSV
    if all_features:
        fieldnames = ['zpid', '_fetchMethod', '_zipcodeHint'] + FeatureExtractor.FEATURES
        with open(output_csv, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_features)
        print(f"\n✓ Exported {len(all_features)} records to {output_csv}")
    else:
        print("No features extracted.")

    if skipped_location:
        print(f"Skipped {skipped_location} records due to location constraints")


if __name__ == '__main__':
    root = Path(__file__).parent
    
    print("=" * 70)
    print("GRAPHQL EXTRACTOR")
    print("=" * 70)
    process_directory(
        root / 'graphql',
        root / 'graphql_features.csv'
    )
    
    print("\n" + "=" * 70)
    print("PROPERTY (RAPIDAPI) EXTRACTOR")
    print("=" * 70)
    process_directory(
        root / 'property',
        root / 'property_features.csv'
    )
    
    print("\n" + "=" * 70)
    print("EXTRACTION COMPLETE")
    print("=" * 70)
