import pandas as pd
import boto3
import os
from io import StringIO
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import traceback

# Load env vars
load_dotenv()

# AWS + MySQL credentials
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = "ratingstesting"
S3_OBJECT_KEY = "ratings_rule.py"

DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# DB engine
connection_str = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(connection_str)

# Fetch and execute validations from S3
def load_validation_functions():
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_OBJECT_KEY)
        script_content = response["Body"].read().decode("utf-8")
        exec(script_content, globals())
        print("\n✅ Validation functions loaded from S3.")
        return True
    except Exception as e:
        print(f"\n❌ Error loading validation functions: {e}")
        return False

# Apply validation logic
def apply_validations(df, file_name, expected_week):
    issues = []
    df.columns = df.columns.str.strip().str.lower()

    try:
        if "clean_product_id" in globals() and "product_id" in df.columns:
            df["product_id"] = df["product_id"].astype(str).apply(globals()["clean_product_id"])
            print("✅ Applied clean_product_id")
    except Exception as e:
        issues.append(f"clean_product_id failed: {e}")

    try:
        if "standardize_date" in globals() and "scraped_date" in df.columns:
            df = globals()["standardize_date"](df, "scraped_date", "scraped_week", "scraped_year")
            print("✅ Standardized scraped_date")
    except Exception as e:
        issues.append(f"standardize_date failed: {e}")

    try:
        if "clean_text" in globals():
            for col in ["brand", "brand_tag", "f_category"]:
                if col in df.columns:
                    df[col] = df[col].apply(globals()["clean_text"])
                    print(f"✅ Cleaned text for: {col}")
    except Exception as e:
        issues.append(f"clean_text failed: {e}")

    try:
        if "remove_duplicates" in globals():
            before = len(df)
            df = globals()["remove_duplicates"](df)
            print(f"✅ Removed {before - len(df)} duplicates")
    except Exception as e:
        issues.append(f"remove_duplicates failed: {e}")

    try:
        if "correct_count_overall" in globals():
            df = globals()["correct_count_overall"](df, file_name)
    except Exception as e:
        issues.append(f"correct_count_overall failed: {e}")

    try:
        if "report_zero_ratings" in globals():
            zeros = globals()["report_zero_ratings"](df)
            print(f"✅ Zero rating rows: {len(zeros)}")
    except Exception as e:
        issues.append(f"report_zero_ratings failed: {e}")

    try:
        if "validate_scraped_week" in globals():
            globals()["validate_scraped_week"](df, expected_week)
    except Exception as e:
        issues.append(str(e))

    try:
        if "validate_rating_dependency" in globals():
            globals()["validate_rating_dependency"](df, file_name)
    except Exception as e:
        issues.append(str(e))

    if issues:
        df["validation_issues"] = "\n".join(issues)
    return df, issues

# Upload to S3
def upload_to_s3(data, filename, folder=""):
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    key = f"{folder}{filename}"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=csv_buffer.getvalue())
    print(f"✅ Uploaded to S3: s3://{S3_BUCKET}/{key}")

# Save to MySQL
def save_to_database(df, table_name="ratings_test"):
    with engine.begin() as conn:
        df = df.drop(columns=["brand_v2", "validation_issues"], errors="ignore")
        df.to_sql(table_name, con=conn, if_exists="append", index=False)
    print(f"✅ Data inserted into MySQL → {table_name}")

# Log file name to ratings_files table
def log_uploaded_file(file_name):
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO ratings_files (file_name) VALUES (:f)"), {"f": file_name})
    print("✅ Logged uploaded file.")

# View uploaded files from DB
def view_uploaded_files():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT file_name, real_time FROM ratings_files ORDER BY real_time DESC LIMIT 10"))
        files = result.fetchall()
        if not files:
            print("⚠ No uploaded files found.")
        else:
            print("\n📁 Last 10 Uploaded Files:")
            for f in files:
                print(f"   • {f.file_name} → {f.real_time}")

# Main process
def main():
    print("\n📌 Choose Action:\n1️⃣ Upload Data\n2️⃣ View Uploaded Files")
    choice = input("Enter choice (1 or 2): ").strip()

    if choice == "2":
        view_uploaded_files()
        return

    elif choice == "1":
        week = input("\n📆 Enter week number (e.g., 12): ").strip().replace("wk", "").zfill(2)
        week_folder = f"wk{week}/"

        file_path = input("\n📄 Enter full file path (CSV): ").strip().strip('"').strip("'")

        if not os.path.isfile(file_path):
            print("❌ File does not exist.")
            return

        if not load_validation_functions():
            print("❌ Aborting due to validation load error.")
            return

        df = pd.read_csv(file_path)
        print(f"\n🔍 Rows in file: {len(df)}")

        file_name = os.path.basename(file_path)
        df, issues = apply_validations(df, file_name, expected_week=week)

        if issues:
            failed_name = f"FAILED_{file_name}"
            upload_to_s3(df, failed_name, folder=week_folder)
            print(f"❌ Validation failed. Issues written to: {failed_name}")
            return

        upload_to_s3(df, file_name, folder=week_folder)
        save_to_database(df)
        log_uploaded_file(file_name)

        print("\n✅ All steps completed successfully.")
    else:
        print("❌ Invalid option. Choose 1 or 2.")

# Run main
if __name__ == "__main__":
    main()

