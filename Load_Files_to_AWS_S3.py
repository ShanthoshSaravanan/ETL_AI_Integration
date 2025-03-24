#step-1
import boto3

# AWS Credentials (Ensure you've configured these using AWS CLI or environment variables)
aws_access_key = "*******************"
aws_secret_key = "*************************"
region_name = "eu-north-1"

# Initialize S3 client
s3 = boto3.resource(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name,
)

# Define S3 bucket and file path
bucket_name = "sandy-databricks-bucket"
raw_file_path = "/content/cleaned_airline_data.csv"  # Path in Colab
s3_file_key = "airline_traffic/clean_data.csv"  # S3 key

# Upload File
s3.Bucket(bucket_name).upload_file(Filename=raw_file_path, Key=s3_file_key)

print(f"✅ Raw file uploaded to S3: s3://{bucket_name}/{s3_file_key}")
