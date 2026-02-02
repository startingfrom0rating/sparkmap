
import geopandas as gpd
import pandas as pd
from pathlib import Path
import json

def augment_pois():
    base_dir = Path('c:/Users/Xxran/.gemini/antigravity/playground/ruby-lagoon/mapbox_ready')
    tracts_path = base_dir / 'maryland_tracts_with_scores.geojson'
    
    print("Loading tracts...")
    tracts = gpd.read_file(tracts_path)
    # Dissolve to counties to make join faster and cleaner (optional but good for consistency)
    # Actually just keep tracts but subset columns
    counties = tracts[['geometry', 'county_name']].dissolve(by='county_name').reset_index()
    
    poi_files = ['hospitals.geojson', 'schools.geojson', 'parks.geojson', 'libraries.geojson', 'stores.geojson']
    
    for filename in poi_files:
        path = base_dir / filename
        if not path.exists():
            print(f"Skipping {filename} (not found)")
            continue
            
        print(f"Processing {filename}...")
        try:
            pois = gpd.read_file(path)
            
            # Ensure CRS match
            if pois.crs != counties.crs:
                pois = pois.to_crs(counties.crs)
            
            # Spatial join to get county_name
            # using 'intersects' to catch things on borders
            joined = gpd.sjoin(pois, counties, how='left', predicate='intersects')
            
            # Handle duplicates (features intersecting multiple counties)
            # We'll just take the first match for simplicity
            joined = joined[~joined.index.duplicated(keep='first')]
            
            # Clean up columns (remove index_right)
            if 'index_right' in joined.columns:
                joined = joined.drop(columns=['index_right'])
            
            # Fill NaN county_name with '' to avoid JSON issues
            joined['county_name'] = joined['county_name'].fillna('')
            
            # Save back
            joined.to_file(path, driver='GeoJSON')
            print(f"  Saved {len(joined)} features with county info.")
            
        except Exception as e:
            print(f"  Error processing {filename}: {e}")

if __name__ == "__main__":
    augment_pois()
