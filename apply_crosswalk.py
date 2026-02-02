"""
Fill Missing Census Tract Data Using 2010→2020 Crosswalk

This script:
1. Loads the Census Bureau's 2010-2020 tract relationship file
2. For each 2020 tract missing data, finds its 2010 parent tract(s)
3. Assigns the parent tract's scores to the child tract
4. Regenerates the GeoJSON with complete data
"""

import pandas as pd
import geopandas as gpd
from pathlib import Path

PROJECT_ROOT = Path(r"C:\Users\Xxran\.gemini\antigravity\playground\ruby-lagoon")

# ============================================
# Step 1: Load crosswalk
# ============================================
print("=" * 60)
print("Step 1: Loading 2010-2020 tract crosswalk...")
print("=" * 60)

crosswalk = pd.read_csv(
    PROJECT_ROOT / "data" / "tract_crosswalk_2020_2010.txt",
    sep='|',
    dtype=str
)

# Filter for Maryland
md_xwalk = crosswalk[crosswalk['GEOID_TRACT_20'].str.startswith('24')].copy()

# Create mapping: for each 2020 tract, find the 2010 tract with largest area overlap
# (use AREALAND_PART as weight)
md_xwalk['AREALAND_PART'] = pd.to_numeric(md_xwalk['AREALAND_PART'], errors='coerce')

# Get the 2010 tract with largest area for each 2020 tract
best_match = (
    md_xwalk
    .sort_values('AREALAND_PART', ascending=False)
    .groupby('GEOID_TRACT_20')
    .first()
    .reset_index()
)[['GEOID_TRACT_20', 'GEOID_TRACT_10']]

print(f"Crosswalk loaded: {len(best_match)} 2020 tracts mapped to 2010 tracts")

# ============================================
# Step 2: Load synthesized data (2010-based)
# ============================================
print("\n" + "=" * 60)
print("Step 2: Loading synthesized data...")
print("=" * 60)

synth = pd.read_csv(
    PROJECT_ROOT / "data" / "spark_map_synthesized.csv",
    dtype={"GEOID": str}
)

# Get Maryland, latest year per tract
md_synth = synth[synth["state_name"] == "Maryland"].copy()
md_synth["year"] = pd.to_numeric(md_synth["year"], errors="coerce")
md_latest = md_synth.sort_values("year", ascending=False).groupby("GEOID").first().reset_index()

print(f"Synthesized data: {len(md_latest)} tracts (2010 definitions)")

# ============================================
# Step 3: Load current GeoJSON (2020-based)
# ============================================
print("\n" + "=" * 60)
print("Step 3: Loading current tract GeoJSON...")
print("=" * 60)

gdf = gpd.read_file(PROJECT_ROOT / "mapbox_ready" / "maryland_tracts_with_scores.geojson")
missing_before = gdf['kfr_pooled_pooled_mean'].isna().sum()
print(f"Total tracts: {len(gdf)}")
print(f"Missing data before crosswalk: {missing_before}")

# ============================================
# Step 4: Fill missing data using crosswalk
# ============================================
print("\n" + "=" * 60)
print("Step 4: Filling missing data via crosswalk...")
print("=" * 60)

# Merge crosswalk to get 2010 GEOID for each 2020 tract
gdf = gdf.merge(
    best_match.rename(columns={'GEOID_TRACT_20': 'GEOID', 'GEOID_TRACT_10': 'GEOID_2010'}),
    on='GEOID',
    how='left'
)

# Get synthetic data keyed by 2010 GEOID
synth_2010 = md_latest.set_index('GEOID')

# Columns to fill
score_cols = [c for c in md_latest.columns if c not in ['GEOID', 'year', 'state_name']]

filled_count = 0
for idx, row in gdf.iterrows():
    # If this tract has missing data
    if pd.isna(row.get('kfr_pooled_pooled_mean')):
        geoid_2010 = row.get('GEOID_2010')
        if geoid_2010 and geoid_2010 in synth_2010.index:
            # Copy data from 2010 parent tract
            parent_data = synth_2010.loc[geoid_2010]
            for col in score_cols:
                if col in gdf.columns and col in parent_data.index:
                    gdf.at[idx, col] = parent_data[col]
            filled_count += 1

print(f"Filled {filled_count} tracts using 2010 parent data")

# Check remaining missing
missing_after = gdf['kfr_pooled_pooled_mean'].isna().sum()
print(f"Missing data after crosswalk: {missing_after}")

# ============================================
# Step 5: Save updated GeoJSON
# ============================================
print("\n" + "=" * 60)
print("Step 5: Saving updated GeoJSON...")
print("=" * 60)

# Drop helper column
gdf = gdf.drop(columns=['GEOID_2010'], errors='ignore')

output_path = PROJECT_ROOT / "mapbox_ready" / "maryland_tracts_with_scores.geojson"
gdf.to_file(output_path, driver="GeoJSON")
print(f"Saved: {output_path}")
print(f"File size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")

# ============================================
# Summary
# ============================================
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Tracts filled with 2010 parent data: {filled_count}")
print(f"Remaining missing (water-only, etc.): {missing_after}")
print(f"Data coverage: {100 * (len(gdf) - missing_after) / len(gdf):.1f}%")
print("\nRefresh http://localhost:8080 to see updated map!")
