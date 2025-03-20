import pandas as pd
import json
import requests  # Fetching JSON rules from S3/GDrive
import boto3
import os
from io import StringIO
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS S3 Credentials
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")

# MySQL Database Credentials
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Database Connection String
connection_str = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(connection_str)

# Fetch validation rules from JSON (AWS S3 or Google Drive)
def load_validation_rules():
    json_url = "https://your_gdrive_or_s3_link.com/validation_rules.json"  # Update with actual link
    response = requests.get(json_url)
    return response.json()

# Function to validate data
def validate_data(df, rules):
    report = []
    
    for rule in rules["rules"]:
        column = rule["column"]
        conditions = rule["conditions"]

        if column not in df.columns:
            report.append(f"Missing column: {column}")
            continue

        for condition in conditions:
            if condition == "no_nulls" and df[column].isnull().sum() > 0:
                report.append(f"{df[column].isnull().sum()} missing values in '{column}'.")
            elif condition.startswith("min_length:"):
                min_len = int(condition.split(":")[1])
                if df[column].apply(lambda x: len(str(x)) < min_len).any():
                    report.append(f"Some entries in '{column}' are shorter than {min_len} characters.")
            elif condition == "valid_email":
                if df[column].str.contains(r"[^@]+@[^@]+\.[^@]+", regex=True).any():
                    report.append(f"'{column}' contains invalid email addresses.")
            elif condition.startswith("greater_than:"):
                min_value = int(condition.split(":")[1])
                if df[column].apply(lambda x: float(x)) <= min_value).any():
                    report.append(f"'{column}' contains values less than or equal to {min_value}.")

    return report

# Upload validated data to AWS S3
def upload_to_s3(data, filename):
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=csv_buffer.getvalue())

# Save validated data to MySQL `ratings_test` table
def save_to_database(df, table_name="ratings_test"):
    with engine.connect() as connection:
        with connection.begin():
            df.to_sql(table_name, con=connection, if_exists="append", index=False)

# Main function to process the file
def process_file(file_path):
    df = pd.read_csv(file_path)
    
    # Fetch rules
    rules = load_validation_rules()
    
    # Validate data
    report = validate_data(df, rules)

    if report:
        print("Validation failed with the following issues:")
        for issue in report:
            print(f"- {issue}")
    else:
        print("All validation checks passed! Uploading data...")
        filename = f"validated_data.csv"
        upload_to_s3(df, filename)
        save_to_database(df)
        print(f"Data uploaded to S3 as {filename} and saved to MySQL table `ratings_test`.")

if __name__ == "__main__":
    file_path = "sample.csv"  # Update with actual file path
    process_file(file_path)

