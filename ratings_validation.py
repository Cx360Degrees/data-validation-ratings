import pandas as pd
import boto3
import os
from io import StringIO
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import traceback
import time
from sqlalchemy.types import String
from tqdm import tqdm
import glob

# Load environment variables
load_dotenv()

# AWS + MySQL credentials
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = "ratingstesting"
S3_OBJECT_KEY = "ratings_rule.py"

# Main DB (cx360)
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
connection_str = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
engine = create_engine(connection_str)

# Exception rules
skip_zero_check = ["fancode_ratings", "nathabit_ratings"]
skip_entire_file = ["giva_ratings", "amazon_bestseller"]

# Track inferred sources and review-based fixes
inferred_sources = {}
review_count_fixes = {}

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
        if "validate_scraped_week" in globals():
            globals()["validate_scraped_week"](df, expected_week)
    except Exception as e:
        raise ValueError(f"validate_scraped_week failed: ❌ {e}")

    try:
        if "validate_rating_dependency" in globals():
            globals()["validate_rating_dependency"](df, file_name)
    except Exception as e:
        issues.append(str(e))

    try:
        if "clean_text" in globals():
            for col in ["brand", "brand_tag", "f_category", "source", "pan"]:
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

    if not any(x in file_name.lower() for x in skip_zero_check):
        try:
            if "report_zero_ratings" in globals():
                zeros = globals()["report_zero_ratings"](df)
                print(f"✅ Zero rating rows: {len(zeros)}")
        except Exception as e:
            issues.append(f"report_zero_ratings failed: {e}")

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

# Upsert to DB
def upsert_data(df, table_name, unique_keys, chunk_size=10000):
    start_time = time.time()
    dtype = {"source": String(100), "product_id": String(100)}
    for i in tqdm(range(0, len(df), chunk_size), desc="Processing Chunks", unit="chunk"):
        chunk = df.iloc[i:i + chunk_size]
        with engine.begin() as conn:
            chunk.to_sql("temp_table", con=conn, if_exists='replace', index=False, dtype=dtype)
            update_cols = [col for col in df.columns if col not in unique_keys]
            if update_cols:
                update_stmt = ", ".join([f"`{col}`=VALUES(`{col}`)" for col in update_cols])
                upsert_query = f"""
                    INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])})
                    SELECT {', '.join([f'`{col}`' for col in df.columns])} FROM `temp_table`
                    ON DUPLICATE KEY UPDATE {update_stmt};
                """
            else:
                upsert_query = f"""
                    INSERT IGNORE INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])})
                    SELECT {', '.join([f'`{col}`' for col in df.columns])} FROM `temp_table`;
                """
            conn.execute(text(upsert_query))
    print(f"✅ Upsert completed to {table_name} in {time.time() - start_time:.2f} sec")

# View recent uploaded files from main DB
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

# Process a single file
def process_file(file_path, week, normalized_week, week_folder, total_stats):
    try:
        df = pd.read_csv(file_path)
        file_name = os.path.basename(file_path)

        # ✅ Infer source from file name if missing or inconsistent
        if "source" not in df.columns or df["source"].isnull().all():
            inferred_source = file_name.split("_ratings")[0]
            df["source"] = inferred_source
            inferred_sources[file_name] = inferred_source
            print(f"🔍 Inferred source from filename: {inferred_source}")

        # ✅ Fix: All star ratings = 0 but review_count > 0 → set count_overall = review_count
        fix_count = 0
        if all(col in df.columns for col in ['count_5star', 'count_4star', 'count_3star', 'count_2star', 'count_1star', 'review_count', 'count_overall']):
            zero_star_mask = (
                (df['count_5star'].fillna(0) == 0) & (df['count_4star'].fillna(0) == 0) &
                (df['count_3star'].fillna(0) == 0) & (df['count_2star'].fillna(0) == 0) &
                (df['count_1star'].fillna(0) == 0)
            )
            review_not_zero = df['review_count'].fillna(0) > 0
            condition = zero_star_mask & review_not_zero
            fix_count = condition.sum()
            df.loc[condition, 'count_overall'] = df.loc[condition, 'review_count']
            if fix_count > 0:
                review_count_fixes[file_name] = fix_count
                print(f"🔄 Updated count_overall using review_count for {fix_count} rows.")

        if any(x in file_name.lower() for x in skip_entire_file):
            print(f"⏭️ Skipping file: {file_name} (per skip_entire_file rule)")
            total_stats['skipped'].append(file_name)
            return

        print(f"\n📄 Processing file: {file_name} → {len(df)} rows")

        if 'scraped_week' in df.columns:
            df['scraped_week'] = df['scraped_week'].astype(str).str.strip().apply(
                lambda x: str(int(float(x))).zfill(2) if x.replace('.', '', 1).isdigit() else x)
            unique_weeks = set(df['scraped_week'].dropna().unique())
            print(f"🗓️ Unique weeks in file: {unique_weeks}")
            if normalized_week not in unique_weeks:
                print(f"⛔ Skipping file due to week mismatch. Expected: {normalized_week}")
                total_stats['skipped'].append(file_name)
                return

        df, issues = apply_validations(df, file_name, expected_week=normalized_week)

        if issues:
            failed_name = f"FAILED_{file_name}"
            upload_to_s3(df, failed_name, folder=week_folder)
            print(f"❌ Validation failed. Issues written to: {failed_name}")
            total_stats['failed'].append((file_name, issues))
            return

        upload_to_s3(df, file_name, folder=week_folder)
        unique_keys = ['source', 'product_id', 'scraped_week', 'scraped_year']

        if any(x in file_name.lower() for x in skip_zero_check):
            upsert_data(df, 'ratings_stage', unique_keys)
            print(f"✅ Inserted ALL rows into ratings_stage (no missing split for: {file_name})")
        else:
            df_missing = df[(df['count_5star'].fillna(0) == 0) & (df['count_4star'].fillna(0) == 0) &
                            (df['count_3star'].fillna(0) == 0) & (df['count_2star'].fillna(0) == 0) &
                            (df['count_1star'].fillna(0) == 0)]
            df_valid = df[~df.index.isin(df_missing.index)]

            if not df_valid.empty:
                upsert_data(df_valid, 'ratings_stage', unique_keys)
            if not df_missing.empty:
                df_missing = df_missing[['source', 'product_id', 'scraped_week', 'scraped_year']]
                upsert_data(df_missing, 'ratings_missing', unique_keys)

        print(f"✅ File completed: {file_name}")
        total_stats['passed'].append(file_name)

    except Exception as e:
        print(f"❌ Failed processing {file_path}: {e}")
        total_stats['failed'].append((os.path.basename(file_path), str(e)))

# Main
def main():
    print("\n📌 Choose Action:\n1️⃣ Upload Data\n2️⃣ View Uploaded Files")
    choice = input("Enter choice (1 or 2): ").strip()

    if choice == "2":
        view_uploaded_files()
        return

    elif choice == "1":
        week = input("\n📆 Enter week number (e.g., 12): ").strip().replace("wk", "")
        normalized_week = str(int(week)).zfill(2)
        week_folder = f"wk{int(week)}/"

        mode = input("\n📁 Upload Mode → Enter 'file' for single file or 'folder' for bulk upload: ").strip().lower()
        files_to_process = []

        if mode == 'folder':
            folder_path = input("📂 Enter folder path: ").strip().strip('"').strip("'")
            if not os.path.isdir(folder_path):
                print("❌ Invalid folder path.")
                return
            files_to_process = glob.glob(os.path.join(folder_path, "*.csv"))
        else:
            file_path = input("\n📄 Enter full file path (CSV): ").strip().strip('"').strip("'")
            if not os.path.isfile(file_path):
                print("❌ File does not exist.")
                return
            files_to_process = [file_path]

        if not load_validation_functions():
            print("❌ Aborting due to validation load error.")
            return

        total_stats = {'passed': [], 'failed': [], 'skipped': []}

        print(f"\n🔍 Found {len(files_to_process)} file(s) to process.")
        for file_path in files_to_process:
            process_file(file_path, week, normalized_week, week_folder, total_stats)

        print("\n📊 Final Summary:")
        print(f"📦 Total Files Processed: {len(files_to_process)}")
        print(f"✅ Successful Files: {len(total_stats['passed'])}")
        print(f"⛔ Skipped Files (week mismatch or rule): {len(total_stats['skipped'])}")
        print(f"❌ Failed Files: {len(total_stats['failed'])}")
        for fname, reason in total_stats['failed']:
            print(f"   - {fname} → {reason}")

        if inferred_sources:
            print("\n🔍 Inferred Sources:")
            for fname, src in inferred_sources.items():
                print(f"   • {fname} → source inferred as '{src}'")

        if review_count_fixes:
            print("\n🔧 Rows auto-fixed using review_count:")
            for fname, count in review_count_fixes.items():
                print(f"   • {fname} → {count} row(s) updated (count_overall = review_count)")

    else:
        print("❌ Invalid option. Choose 1 or 2.")

if __name__ == "__main__":
    main()


