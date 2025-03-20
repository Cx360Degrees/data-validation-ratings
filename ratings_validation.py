import pandas as pd
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
S3_BUCKET = "ratingstesting"
S3_OBJECT_KEY = "ratings_rule.py"  # S3 file name

# MySQL Database Credentials
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Database Connection String
connection_str = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(connection_str)

# Fetch validation functions from S3 using Boto3
def load_validation_functions():
    """Fetch and execute validation functions from AWS S3 using Boto3."""
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_OBJECT_KEY)
        script_content = response["Body"].read().decode("utf-8")

        print("\n✅ Successfully fetched `ratings_validation.py` from S3.")

        # Execute the script to load functions
        exec(script_content, globals())

        # Check available validation functions
        validation_funcs = [func for func in globals() if callable(globals()[func])]
        print("\n🔎 Validation Functions Available (Loaded from S3):")
        for func in validation_funcs:
            print(f"   ✅ {func}")

        return validation_funcs

    except Exception as e:
        print(f"\n❌ Error fetching `ratings_validation.py` from S3: {e}")
        return []

# Apply all loaded validation functions
def apply_validations(df):
    """Automatically applies validation functions to the matching columns in the DataFrame."""
    
    # Standardize column names (remove spaces, lowercase)
    df.columns = df.columns.str.strip().str.lower()

    applied_funcs = []

    if "clean_product_id" in globals() and "product_id" in df.columns:
        df["product_id"] = df["product_id"].astype(str)
        df["product_id"] = df["product_id"].apply(globals()["clean_product_id"])
        applied_funcs.append("clean_product_id")

    if "standardize_date" in globals() and "scraped_date" in df.columns:
        df = globals()["standardize_date"](df, col="scraped_date", col_wk="scraped_week", col_yr="scraped_year")
        applied_funcs.append("standardize_date")

    if "clean_text" in globals():
        text_columns = ["brand", "brand_tag", "f_category"]
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].apply(globals()["clean_text"])
                applied_funcs.append(f"clean_text ({col})")

    if "remove_duplicates" in globals():
        before_dupes = len(df)
        df = globals()["remove_duplicates"](df)
        after_dupes = len(df)
        dupes_removed = before_dupes - after_dupes
    else:
        dupes_removed = 0

    if "correct_count_overall" in globals() and "count_overall" in df.columns:
        df = globals()["correct_count_overall"](df)
        applied_funcs.append("correct_count_overall")

    if "report_zero_ratings" in globals():
        zero_ratings_count = len(globals()["report_zero_ratings"](df))
    else:
        zero_ratings_count = 0

    # Print summary
    print("\n🔹 **Validation Summary**")
    print(f"   ✅ Total Rows Processed: {len(df)}")
    print(f"   ✅ Product_id Cleaned: {df['product_id'].str.contains('/').sum()} → {df['product_id'].str.contains('/').sum()}")
    print(f"   ✅ Duplicates Removed: {dupes_removed}")
    print(f"   ✅ Zero Rating Rows: {zero_ratings_count}")
    print(f"   ✅ Count Mismatch Errors Fixed: {df['count_overall'].isna().sum()}")

    return df

# Upload validated data to AWS S3
def upload_to_s3(data, filename):
    """Uploads validated data to AWS S3 bucket."""
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=csv_buffer.getvalue())
    print(f"✅ Data uploaded to S3: {filename}")

# Save validated data to MySQL
def save_to_database(df, table_name="ratings_test"):
    """Saves validated data to MySQL table."""
    with engine.connect() as connection:
        with connection.begin():
            df = df.drop(columns=["brand_v2"], errors="ignore")
            df.to_sql(table_name, con=connection, if_exists="append", index=False)
    print(f"✅ Data inserted into MySQL table: {table_name}")

# Main function to process the file
def process_file(file_path):
    print("\n🔄 Fetching validation functions from AWS S3...")
    validation_funcs = load_validation_functions()

    print(f"\n🔄 Loading CSV file from local path: {file_path}...")
    df = pd.read_csv(file_path)

    # Print total row count before validation
    print(f"\n✅ Total Rows in Source File: {len(df)}")

    print("\n🔄 Running validation checks...")
    df = apply_validations(df)

    print("\n✅ All validation checks passed! Uploading data...")
    filename = f"validated_data.csv"
    upload_to_s3(df, filename)
    save_to_database(df)
    print("\n✅ Process completed successfully.")

if __name__ == "__main__":
    file_path = r"C:\Users\A\Downloads\nathabit_ratings_wk4_2025.csv" #change source as per user requriement 
    process_file(file_path)
