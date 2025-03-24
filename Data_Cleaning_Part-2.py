import pandas as pd
import csv

# Load dataset
file_path = "/content/drive/MyDrive/Dataset/Air_Traffic.csv"  # Update with your actual path
df = pd.read_csv(file_path)

# Step 1: Standardize column names (replace spaces with underscores)
df.columns = df.columns.str.strip().str.replace(" ", "_")

# List of columns expected to be in date format
date_columns = ["Activity_Period_Start_Date", "data_as_of", "data_loaded_at"]

# Function to clean date formats, handling datetime values
def clean_date_format(date_value):
    if pd.isna(date_value) or date_value.strip() == "":
        return None  # Handle empty values
    
    try:
        # Convert to datetime (auto-detects most formats)
        return pd.to_datetime(date_value, errors='coerce').strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        print(f"Failed to parse: {date_value} -> {e}")
        return None  # Mark invalid values as NaN

# Apply cleaning function to each date column
for col in date_columns:
    df[col] = df[col].astype(str).apply(clean_date_format)

# Check for data presence
print(f"Rows before dropping: {df.shape[0]}")
df.dropna(subset=date_columns, inplace=True)
print(f"Rows after dropping: {df.shape[0]}")

# Step 3: Save cleaned data
cleaned_file = "cleaned_airline_data.csv"

if not df.empty:
    df.to_csv(cleaned_file, index=False, quoting=csv.QUOTE_ALL)
    print(f"✅ Cleaned file saved: {cleaned_file}")
else:
    print("❌ No valid data after cleaning. Check source file.")
