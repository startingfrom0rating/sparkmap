"""
Spark Map Data Synthesis - Lean Version
Extracts only key metrics from each dataset to avoid bloat.
"""
import pandas as pd
import glob
import os

# Key columns to extract from Opportunity Atlas
# Focus on pooled (all demographics) mean outcomes
OA_COLS = [
    'state', 'county', 'tract',
    # Economic mobility (household income rank)
    'kfr_pooled_pooled_mean',
    'kfr_pooled_pooled_p25',
    'kfr_pooled_pooled_p75',
    # Employment
    'working_pooled_pooled_mean',
    # Incarceration
    'jail_pooled_pooled_mean',
    # College attendance
    'college_pooled_pooled_mean',
    # Teen birth rate
    'teenbrth_pooled_pooled_mean',
    # Staying in same tract
    'stayhome_pooled_pooled_mean',
]

# Key columns from COI (composite indices only)
COI_COLS = [
    'geoid10', 'year', 'state_name', 'county_name',
    # Overall COI scores
    'z_COI_nat', 'r_COI_nat',
    # Domain scores (Education, Health, Social/Economic)
    'z_ED_nat', 'z_HE_nat', 'z_SE_nat',
    'r_ED_nat', 'r_HE_nat', 'r_SE_nat',
]

def load_opportunity_atlas_data():
    """Load OA with only key columns."""
    print("Loading Opportunity Atlas (key columns only)...")
    files = glob.glob("data/opportunity_atlas/*.csv")
    if not files:
        return None
    
    # Load only necessary columns
    try:
        df = pd.read_csv(files[0], usecols=OA_COLS, low_memory=False)
    except ValueError as e:
        # Some columns might not exist, load what we can
        print(f"Warning: {e}")
        df = pd.read_csv(files[0], nrows=0)
        available = [c for c in OA_COLS if c in df.columns]
        df = pd.read_csv(files[0], usecols=available, low_memory=False)
    
    # Create GEOID
    df['GEOID'] = (
        df['state'].astype(str).str.zfill(2) + 
        df['county'].astype(str).str.zfill(3) + 
        df['tract'].astype(str).str.zfill(6)
    )
    
    # Drop construction columns
    df.drop(['state', 'county', 'tract'], axis=1, inplace=True, errors='ignore')
    
    print(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
    return df

def load_coi_data():
    """Load COI with only key columns."""
    print("Loading COI (key columns only)...")
    files = glob.glob("data/coi_data/data.csv")
    if not files:
        return None
    
    try:
        df = pd.read_csv(files[0], usecols=COI_COLS, low_memory=False)
    except ValueError as e:
        print(f"Warning: {e}")
        df = pd.read_csv(files[0], nrows=0)
        available = [c for c in COI_COLS if c in df.columns]
        df = pd.read_csv(files[0], usecols=available, low_memory=False)
    
    # Rename geoid10 to GEOID
    if 'geoid10' in df.columns:
        df['GEOID'] = df['geoid10'].astype(str).str.zfill(11)
        df.drop('geoid10', axis=1, inplace=True)
    
    print(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
    return df

def load_close_city_data():
    """Load Close.city walkability data."""
    print("Loading Close.city walkability...")
    result_path = "data/close_city/results/travel_time_results.csv"
    if not os.path.exists(result_path):
        print("  Warning: Close.city data not found")
        return None
    
    df = pd.read_csv(result_path)
    df['GEOID'] = df['GEOID'].astype(str).str.zfill(11)
    
    # Pivot to wide format
    df_pivot = df.pivot_table(
        index='GEOID', 
        columns='type', 
        values='travel_time', 
        aggfunc='mean'
    ).reset_index()
    df_pivot.columns.name = None
    
    # Aggregate block to tract if needed
    if len(df_pivot['GEOID'].iloc[0]) > 11:
        df_pivot['GEOID'] = df_pivot['GEOID'].str[:11]
        df_pivot = df_pivot.groupby('GEOID').mean(numeric_only=True).reset_index()
    
    print(f"  Loaded {len(df_pivot)} rows, {len(df_pivot.columns)} columns")
    return df_pivot

def main():
    oa_df = load_opportunity_atlas_data()
    coi_df = load_coi_data()
    walk_df = load_close_city_data()
    
    # Start with OA as base
    final_df = oa_df
    
    # Merge COI
    if coi_df is not None and final_df is not None:
        final_df = pd.merge(final_df, coi_df, on='GEOID', how='left')
        print(f"After COI merge: {final_df.shape}")
    
    # Merge walkability
    if walk_df is not None and final_df is not None:
        final_df = pd.merge(final_df, walk_df, on='GEOID', how='left')
        print(f"After walkability merge: {final_df.shape}")
    
    if final_df is not None:
        output_file = "data/spark_map_synthesized.csv"
        final_df.to_csv(output_file, index=False)
        size_mb = os.path.getsize(output_file) / (1024 * 1024)
        print(f"\nSaved to {output_file}")
        print(f"Final: {len(final_df)} rows, {len(final_df.columns)} columns, {size_mb:.2f} MB")
    else:
        print("Error: No data to synthesize")

if __name__ == "__main__":
    main()
