import os
import pandas as pd
import hashlib
import time
from datetime import datetime
from transformers import pipeline  # Using an AI model for anomaly detection & insights

# Define schema paths
base_path = "/content/test_db"
staging_sch = os.path.join(base_path, "staging_sch")
cdc_sch = os.path.join(base_path, "cdc_sch")
scd_sch = os.path.join(base_path, "scd_sch")
audit_sch = os.path.join(base_path, "audit_sch")

# Ensure all directories exist
for path in [staging_sch, cdc_sch, scd_sch, audit_sch]:
    os.makedirs(path, exist_ok=True)

# AI-based anomaly detection model (for later use in audit logs)
anomaly_detector = pipeline("text-classification", model="facebook/bart-large-mnli")

def identify_primary_key(df):
    """AI-driven approach to detecting primary keys based on uniqueness."""
    for col in df.columns:
        if df[col].nunique() == len(df):  # Unique values match total count
            return [col]  # Single column PK
    return [df.columns[0]]  # Default to first column if no clear PK

def scd_2_process():
    """Handles SCD-2 processing and metadata updates."""
    metadata_records = []
    
    for file in os.listdir(staging_sch):
        if file.endswith(".csv"):
            table_name = file.split(".")[0]
            staging_path = os.path.join(staging_sch, file)
            scd_path = os.path.join(scd_sch, f"scd_{file}")
            
            # Load staging data
            df_staging = pd.read_csv(staging_path)
            primary_key = identify_primary_key(df_staging)
            start_time = time.time()
            
            if os.path.exists(scd_path):
                df_scd = pd.read_csv(scd_path)
                
                # Compare records
                df_merged = df_staging.merge(df_scd, on=primary_key, how='outer', indicator=True)
                
                new_records = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])
                
                if not new_records.empty:
                    new_records['OMD_EFFECTIVE_DATE'] = datetime.now()
                    new_records['OMD_EXPIRY_DATE'] = None
                    new_records['OMD_CURRENT_RECORD_IND'] = 1
                    
                    df_scd['OMD_CURRENT_RECORD_IND'] = 0  # Expire old records
                    df_scd['OMD_EXPIRY_DATE'] = datetime.now()
                    
                    df_scd = pd.concat([df_scd, new_records])
                    df_scd.to_csv(scd_path, index=False)
                    
                    # Audit log entry
                    audit_entry = f"New {len(new_records)} records added to {table_name} in SCD-2 processing."
                    print(audit_entry)  # Log output
                    
            else:
                # Create initial SCD-2 table
                df_staging['OMD_EFFECTIVE_DATE'] = datetime.now()
                df_staging['OMD_EXPIRY_DATE'] = None
                df_staging['OMD_CURRENT_RECORD_IND'] = 1
                df_staging.to_csv(scd_path, index=False)
                print(f"SCD-2 table {table_name} created.")
            
            # Metadata logging
            total_records = len(pd.read_csv(scd_path))
            execution_time = round(time.time() - start_time, 2)
            
            metadata_records.append(["test_db", "scd_sch", table_name, ",".join(primary_key), total_records, execution_time, "Success"])
    
    # Save metadata records
    metadata_df = pd.DataFrame(metadata_records, columns=["Database Name", "Schema Name", "Table Name", "Primary Key", "Total Record Count", "Execution Duration", "Execution Status"])
    metadata_path = os.path.join(audit_sch, "control_table.csv")
    metadata_df.to_csv(metadata_path, index=False)
    print("Metadata table updated successfully.")

scd_2_process()
