"""
Prepare Spark Map Data for Mapbox Integration

This script:
1. Filters synthesized data for Maryland only
2. Converts the Maryland tract shapefile to GeoJSON
3. Joins tract polygons with opportunity scores
4. Outputs files ready for Mapbox upload

Data "Lenses" (different visualization layers):
- Base: Tract polygons with all metrics (color by different fields)
- Points: Hospitals, Parks, Schools, etc. as overlays
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path
import json

# ============================================
# Configuration
# ============================================
PROJECT_ROOT = Path(r"C:\Users\Xxran\.gemini\antigravity\playground\ruby-lagoon")
DOWNLOADS = Path(r"C:\Users\Xxran\Downloads")

# Input files
SYNTHESIZED_CSV = PROJECT_ROOT / "data" / "spark_map_synthesized.csv"
MD_SHAPEFILE = DOWNLOADS / "tl_2025_24_tract" / "tl_2025_24_tract.shp"
POI_FOLDER = DOWNLOADS / "Spark Map-20260128T002745Z-3-001" / "Spark Map"

# Output directory
OUTPUT_DIR = PROJECT_ROOT / "mapbox_ready"
OUTPUT_DIR.mkdir(exist_ok=True)

# ============================================
# Step 1: Filter synthesized data for Maryland
# ============================================
print("=" * 60)
print("Step 1: Filtering synthesized data for Maryland...")
print("=" * 60)

# Load synthesized data
df = pd.read_csv(SYNTHESIZED_CSV, dtype={"GEOID": str})
print(f"Total rows in synthesized data: {len(df):,}")

# Filter for Maryland
md_df = df[df["state_name"] == "Maryland"].copy()
print(f"Maryland rows: {len(md_df):,}")

# The data has multiple years per GEOID - get the most recent year for each tract
md_df["year"] = pd.to_numeric(md_df["year"], errors="coerce")
md_latest = md_df.sort_values("year", ascending=False).groupby("GEOID").first().reset_index()
print(f"Unique Maryland tracts: {len(md_latest):,}")

# Save Maryland-only CSV
md_csv_path = OUTPUT_DIR / "maryland_synthesized.csv"
md_latest.to_csv(md_csv_path, index=False)
print(f"Saved: {md_csv_path}")

# Show sample of available columns for lenses
print("\nAvailable metrics for 'lenses' (color-coding):")
score_cols = [c for c in md_latest.columns if any(x in c.lower() for x in ['kfr', 'coi', 'z_', 'r_', 'mean', 'working', 'jail', 'stayhome'])]
for col in score_cols[:15]:
    print(f"  - {col}")
print(f"  ... and {len(score_cols) - 15} more" if len(score_cols) > 15 else "")

# ============================================
# Step 2: Load Maryland shapefile and convert to GeoJSON
# ============================================
print("\n" + "=" * 60)
print("Step 2: Processing Maryland tract shapefile...")
print("=" * 60)

# Load shapefile
md_tracts = gpd.read_file(MD_SHAPEFILE)
print(f"Shapefile loaded: {len(md_tracts)} tract polygons")
print(f"Columns: {list(md_tracts.columns)}")

# The shapefile likely has GEOID or GEOID20. Let's check
geoid_col = None
for col in md_tracts.columns:
    if "geoid" in col.lower():
        geoid_col = col
        break

if geoid_col:
    print(f"Using GEOID column: {geoid_col}")
    md_tracts["GEOID"] = md_tracts[geoid_col].astype(str)
else:
    # Build GEOID from state + county + tract
    print("Building GEOID from STATEFP + COUNTYFP + TRACTCE")
    md_tracts["GEOID"] = (
        md_tracts["STATEFP"].astype(str) +
        md_tracts["COUNTYFP"].astype(str) +
        md_tracts["TRACTCE"].astype(str)
    )

print(f"Sample GEOIDs from shapefile: {md_tracts['GEOID'].head(3).tolist()}")
print(f"Sample GEOIDs from CSV: {md_latest['GEOID'].head(3).tolist()}")

# ============================================
# Step 3: Join tract polygons with scores
# ============================================
print("\n" + "=" * 60)
print("Step 3: Joining tract polygons with opportunity scores...")
print("=" * 60)

# Prepare score columns for join (exclude geometry-related columns)
score_cols_to_join = [c for c in md_latest.columns if c != "GEOID"]

# Merge
md_tracts_scored = md_tracts.merge(
    md_latest[["GEOID"] + score_cols_to_join],
    on="GEOID",
    how="left"
)

matched = md_tracts_scored["kfr_pooled_pooled_mean"].notna().sum()
print(f"Tracts matched with scores: {matched} / {len(md_tracts_scored)}")

# Check for unmatched tracts
unmatched = md_tracts_scored[md_tracts_scored["kfr_pooled_pooled_mean"].isna()]["GEOID"]
if len(unmatched) > 0:
    print(f"Unmatched GEOIDs (first 5): {unmatched.head(5).tolist()}")
    
# ============================================
# Step 4: Save as GeoJSON for Mapbox
# ============================================
print("\n" + "=" * 60)
print("Step 4: Saving GeoJSON for Mapbox upload...")
print("=" * 60)

# Convert to WGS84 (EPSG:4326) for Mapbox
md_tracts_scored = md_tracts_scored.to_crs("EPSG:4326")

# Save full tract GeoJSON with all scores
tract_geojson_path = OUTPUT_DIR / "maryland_tracts_with_scores.geojson"
md_tracts_scored.to_file(tract_geojson_path, driver="GeoJSON")
print(f"Saved: {tract_geojson_path}")
print(f"File size: {tract_geojson_path.stat().st_size / 1024 / 1024:.1f} MB")

# ============================================
# Step 5: Copy POI GeoJSON files
# ============================================
print("\n" + "=" * 60)
print("Step 5: Copying POI layers for Mapbox...")
print("=" * 60)

import shutil

poi_files = list(POI_FOLDER.glob("*.geojson"))
for poi_file in poi_files:
    dest = OUTPUT_DIR / poi_file.name
    shutil.copy(poi_file, dest)
    print(f"Copied: {poi_file.name} ({poi_file.stat().st_size / 1024:.1f} KB)")

# ============================================
# Summary
# ============================================
print("\n" + "=" * 60)
print("DONE! Files ready for Mapbox upload:")
print("=" * 60)
print(f"\n📁 Output directory: {OUTPUT_DIR}")
print("\n📊 Tract layer (choropleth):")
print(f"   {tract_geojson_path.name}")
print("\n   Color by these 'lenses':")
print("   - kfr_pooled_pooled_mean (income mobility)")
print("   - z_COI_nat (overall opportunity z-score)")
print("   - z_ED_nat (education domain)")
print("   - z_HE_nat (health/environment domain)")
print("   - z_SE_nat (social/economic domain)")

print("\n📍 Point layers:")
for poi_file in poi_files:
    print(f"   - {poi_file.name}")

print("\n" + "=" * 60)
print("NEXT STEPS:")
print("=" * 60)
print("""
1. Go to Mapbox Studio: https://studio.mapbox.com/

2. Upload Tilesets:
   - Click 'Tilesets' → 'New Tileset' → Upload each file from mapbox_ready/
   
3. Create a Style:
   - New Style → Start from a template (Light or Streets)
   
4. Add Tract Layer (choropleth):
   - Add Layer → Select maryland_tracts_with_scores tileset
   - Type: Fill
   - Color: Data-driven by kfr_pooled_pooled_mean
   
5. Add Point Layers:
   - Add Layer → Select hospitals tileset
   - Type: Circle or Symbol (use Mapbox icons)
   - Repeat for parks, schools, etc.

6. Add Layer Toggle (Lenses):
   - In your web app, use Mapbox GL JS setLayoutProperty()
   - to toggle visibility or change the fill-color data expression
""")
