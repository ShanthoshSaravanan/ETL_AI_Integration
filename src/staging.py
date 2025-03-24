import os
import pandas as pd
import shutil

# Define paths
base_path = "/content/test_db"
staging_path = os.path.join(base_path, "staging_sch")
cdc_path = os.path.join(base_path, "cdc_sch")

# Ensure CDC directory exists
os.makedirs(cdc_path, exist_ok=True)

# Get all CSV files in staging_sch
staging_files = [f for f in os.listdir(staging_path) if f.endswith(".csv")]

if not staging_files:
    print("❌ No files found in staging_sch! Please upload files.")
else:
    print(f"✅ Found {len(staging_files)} file(s) in staging_sch: {staging_files}")

# Process each file in staging_sch
for file in staging_files:
    staging_file_path = os.path.join(staging_path, file)
    
    # Create CDC filename as cdc_<original_filename>.csv
    cdc_filename = f"cdc_{file}"
    cdc_file_path = os.path.join(cdc_path, cdc_filename)

    if not os.path.exists(cdc_file_path):
        # If CDC file does not exist, create it
        shutil.copy(staging_file_path, cdc_file_path)
        print(f"🆕 Created CDC file: {cdc_filename}")
    else:
        # If CDC file exists, update it (append new data)
        df_staging = pd.read_csv(staging_file_path)
        df_cdc = pd.read_csv(cdc_file_path)

        # **AI-Powered Change Detection**
        new_data = df_staging[~df_staging.isin(df_cdc)].dropna(how="all")
        
        if not new_data.empty:
            new_data.to_csv(cdc_file_path, mode="a", index=False, header=False)
            print(f"🔄 Updated {cdc_filename} with {len(new_data)} new rows")
        else:
            print(f"✅ No changes detected for: {file}, CDC is up-to-date!")
