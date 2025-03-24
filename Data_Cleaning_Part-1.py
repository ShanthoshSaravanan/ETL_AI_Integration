import pandas as pd
import boto3


# Load dataset
file_path = "/content/drive/MyDrive/Dataset/Air_Traffic.csv"  # Change this to your actual file path
df = pd.read_csv(file_path)

#  Standardize column names
df.columns = df.columns.str.strip().str.replace(" ", "_")
print(df.head())


