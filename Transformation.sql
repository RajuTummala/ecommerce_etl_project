// --------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE clean.CLEAN_USER_PROCEDURE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'pyarrow',
    'numpy'
)
HANDLER = 'run_user_model_etl'
AS
$$
import re
import ipaddress
import pandas as pd
import numpy as np

def run_user_model_etl(session):
    # ---------------------------------------------------------
    # 1. READ RAW DATA
    # ---------------------------------------------------------
    raw_df = session.table("RAW.USER_MODEL").to_pandas()
    
    if raw_df.empty:
        return "No data found in RAW table."
        
    df = raw_df.copy()
    
    # Strip leading/trailing whitespace across all text columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Track error reasons per row
    df['ERROR_REASONS'] = [[] for _ in range(len(df))]

    # Helper function to append error reasons
    def add_error(mask, reason_msg):
        for idx in df[mask].index:
            df.loc[idx, 'ERROR_REASONS'].append(reason_msg)

    # ---------------------------------------------------------
    # 2. HARD VALIDATION CHECKS (Failures -> Quarantine)
    # ---------------------------------------------------------
    
    # Check 1: USER_ID (Must be present and numeric)
    user_id_num = pd.to_numeric(df["USER_ID"], errors="coerce")
    add_error(user_id_num.isna(), "USER_ID is NULL or non-numeric")
    
    # Check 2: USER_ID Duplicates
    dup_user_ids = user_id_num.duplicated(keep=False) & user_id_num.notna()
    add_error(dup_user_ids, "Duplicate USER_ID in batch")

    # Check 3: Required Name Fields
    add_error(df["FIRST_NAME"].isna() | (df["FIRST_NAME"] == ""), "FIRST_NAME is NULL or empty")
    add_error(df["LAST_NAME"].isna() | (df["LAST_NAME"] == ""), "LAST_NAME is NULL or empty")

    # Check 4: Email Validation
    email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    invalid_email = df["EMAIL"].isna() | (df["EMAIL"] == "") | (df["EMAIL"] == "Null") | ~df["EMAIL"].fillna("").str.match(email_pattern)
    add_error(invalid_email, "EMAIL is NULL, empty, or invalid format")

    # Check 5: Password Hash Rules
    invalid_password = (
        df["PASSWORD_HASH"].isna()
        | (df["PASSWORD_HASH"] == "")
        | df["PASSWORD_HASH"].fillna("").str.isdigit()  # Plain numeric password
        | df["PASSWORD_HASH"].fillna("").str.contains("substring", case=False, na=False)
    )
    add_error(invalid_password, "PASSWORD_HASH is missing, plain numbers, or test string")

    # Check 6: Required Authorization Role
    add_error(df["ROLE_ID"].isna() | (df["ROLE_ID"] == ""), "ROLE_ID is missing")
    add_error(df["ROLE_NAME"].isna() | (df["ROLE_NAME"] == ""), "ROLE_NAME is missing")

    # Check 7: Mandatory Audit Timestamps
    created_at_dt = pd.to_datetime(df["CREATED_AT"], errors="coerce")
    add_error(created_at_dt.isna(), "CREATED_AT timestamp is invalid or NULL")

    # Check 8: FULL_DATE validation
    full_date_dt = pd.to_datetime(df["FULL_DATE"], errors="coerce")
    add_error(full_date_dt.isna(), "FULL_DATE is invalid or NULL")

    # ---------------------------------------------------------
    # 3. SOFT CLEANING & REPAIRS (Optional Fields)
    # ---------------------------------------------------------
    
    # Phone Number Cleaning & Soft Validation
    phone_clean = df["PHONE_NUMBER"].fillna("").str.replace(r"[\s().-]", "", regex=True)
    valid_phone_mask = phone_clean.str.match(r"^\+?\d{10,15}$")
    df["PHONE_NUMBER"] = np.where(valid_phone_mask, phone_clean, None)

    # IP Address Soft Validation
    def validate_ip(x):
        if pd.isna(x) or str(x).strip() == "":
            return None
        try:
            ipaddress.ip_address(str(x).strip())
            return str(x).strip()
        except ValueError:
            return None
            
    df["IP_ADDRESS"] = df["IP_ADDRESS"].apply(validate_ip)

    # Hour, Minute, Second Bounds Repair
    df["HOUR"] = pd.to_numeric(df["HOUR"], errors="coerce")
    df["HOUR"] = np.where(df["HOUR"].between(0, 23), df["HOUR"], None)

    df["MINUTE"] = pd.to_numeric(df["MINUTE"], errors="coerce")
    df["MINUTE"] = np.where(df["MINUTE"].between(0, 59), df["MINUTE"], None)

    df["SECOND"] = pd.to_numeric(df["SECOND"], errors="coerce")
    df["SECOND"] = np.where(df["SECOND"].between(0, 59), df["SECOND"], None)

    # Notification Counts (must be non-negative)
    df["NOTIFICATION_SENT"] = pd.to_numeric(df["NOTIFICATION_SENT"], errors="coerce")
    df["NOTIFICATION_SENT"] = np.where(df["NOTIFICATION_SENT"] >= 0, df["NOTIFICATION_SENT"], None)

    df["NOTIFICATION_READ"] = pd.to_numeric(df["NOTIFICATION_READ"], errors="coerce")
    df["NOTIFICATION_READ"] = np.where(df["NOTIFICATION_READ"] >= 0, df["NOTIFICATION_READ"], None)

    # Status Boolean Conversion
    status_true = df["STATUS"].astype(str).str.upper().isin(["Y", "YES", "TRUE", "1"])
    status_false = df["STATUS"].astype(str).str.upper().isin(["N", "NO", "FALSE", "0"])
    df["STATUS"] = np.where(status_true, True, np.where(status_false, False, None))

    # Is Primary Boolean Conversion
    primary_true = df["IS_PRIMARY"].astype(str).str.upper().isin(["Y", "YES", "TRUE", "1"])
    df["IS_PRIMARY"] = np.where(primary_true, True, False)

    # ---------------------------------------------------------
    # 4. DERIVED FIELDS RECALCULATION
    # ---------------------------------------------------------
    df["FULL_DATE"] = full_date_dt.dt.date
    df["DAY"] = full_date_dt.dt.day
    df["MONTH"] = full_date_dt.dt.month
    df["QUARTER"] = full_date_dt.dt.quarter
    df["YEAR"] = full_date_dt.dt.year
    df["DAY_NAME"] = full_date_dt.dt.day_name()

    # ---------------------------------------------------------
    # 5. SPLIT DATA: CLEAN vs QUARANTINE
    # ---------------------------------------------------------
    has_errors_mask = df["ERROR_REASONS"].apply(lambda errs: len(errs) > 0)

    # Prepare Quarantine Dataframe
    df_quarantine = df[has_errors_mask].copy()
    if not df_quarantine.empty:
        df_quarantine["ERROR_REASON"] = df_quarantine["ERROR_REASONS"].apply(lambda errs: "; ".join(errs))
        # Keep key identifier fields + error details
        quarantine_to_write = df_quarantine[["USER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "ERROR_REASON"]].copy()
        
        # Write to QUARANTINE table
        session.write_pandas(
        quarantine_to_write,
        table_name="USER_MODEL",
        database="ECOMMERCE",
        schema="QUARANTINE",
        auto_create_table=False,
        overwrite=False
        )

    # Prepare Clean Dataframe
    df_clean = df[~has_errors_mask].copy()
    if df_clean.empty:
        return "All records quarantined. No valid data to merge."

    # ---------------------------------------------------------
    # 6. ENFORCE STRICT DATA TYPES ON CLEAN DATAFRAME
    # ---------------------------------------------------------
    numeric_cols = [
        "USER_ID", "ADDRESS_ID", "MOBILE_ID", "ROLE_ID", 
        "DAY", "MONTH", "QUARTER", "YEAR", "HOUR", "MINUTE", "SECOND",
        "ACTIVITY_COUNT", "NOTIFICATION_SENT", "NOTIFICATION_READ"
    ]
    for col in numeric_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Int64")

    df_clean["CREATED_AT"] = pd.to_datetime(
        df_clean["CREATED_AT"],
        errors="coerce"
    )
    
    df_clean["UPDATED_AT"] = pd.to_datetime(
        df_clean["UPDATED_AT"],
        errors="coerce"
    )
    
    # Convert timestamps to Snowflake-compatible strings
    df_clean["CREATED_AT"] = df_clean["CREATED_AT"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    
    df_clean["UPDATED_AT"] = df_clean["UPDATED_AT"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    # Drop temp processing column
    df_clean = df_clean.drop(columns=["ERROR_REASONS"])

    # ---------------------------------------------------------
    # 7. WRITE TO STAGING & EXECUTE MERGE
    # ---------------------------------------------------------
    # Truncate and write to temporary staging table
    session.sql("TRUNCATE TABLE ecommerce.temp.TEMP_CLEAN_USER_MODEL").collect()
    session.write_pandas(
        df_clean,
        table_name="TEMP_CLEAN_USER_MODEL",
        database="ECOMMERCE",
        schema="TEMP",
        auto_create_table=False,
        overwrite=True
    )

    # Trigger MERGE Statement
    merge_sql = """
    MERGE INTO ecommerce.CLEAN.CLEAN_USER_MODEL target
    USING ecommerce.TEMP.temp_CLEAN_USER_MODEL src
    ON target.USER_ID = src.USER_ID
    WHEN MATCHED THEN UPDATE SET
        target.FIRST_NAME        = src.FIRST_NAME,
        target.LAST_NAME         = src.LAST_NAME,
        target.MIDDLE_NAME       = src.MIDDLE_NAME,
        target.EMAIL             = src.EMAIL,
        target.PASSWORD_HASH     = src.PASSWORD_HASH,
        target.STATUS            = src.STATUS,
        target.ADDRESS_ID        = src.ADDRESS_ID,
        target.COUNTRY           = src.COUNTRY,
        target.STATE             = src.STATE,
        target.DISTRICT          = src.DISTRICT,
        target.CITY              = src.CITY,
        target.VILLAGE           = src.VILLAGE,
        target.ZIPCODE           = src.ZIPCODE,
        target.ADDRESS_TYPE      = src.ADDRESS_TYPE,
        target.MOBILE_ID         = src.MOBILE_ID,
        target.COUNTRY_CODE      = src.COUNTRY_CODE,
        target.PHONE_NUMBER      = src.PHONE_NUMBER,
        target.IS_PRIMARY        = src.IS_PRIMARY,
        target.ROLE_ID           = src.ROLE_ID,
        target.ROLE_NAME         = src.ROLE_NAME,
        target.DEVICE_NAME       = src.DEVICE_NAME,
        target.BROWSER           = src.BROWSER,
        target.IP_ADDRESS        = src.IP_ADDRESS,
        target.LOGIN_STATUS      = src.LOGIN_STATUS,
        target.FULL_DATE         = src.FULL_DATE,
        target.DAY               = src.DAY,
        target.MONTH             = src.MONTH,
        target.QUARTER           = src.QUARTER,
        target.YEAR              = src.YEAR,
        target.DAY_NAME          = src.DAY_NAME,
        target.HOUR              = src.HOUR,
        target.MINUTE            = src.MINUTE,
        target.SECOND            = src.SECOND,
        target.ACTIVITY_NAME     = src.ACTIVITY_NAME,
        target.ACTIVITY_COUNT    = src.ACTIVITY_COUNT,
        target.NOTIFICATION_TYPE = src.NOTIFICATION_TYPE,
        target.NOTIFICATION_SENT = src.NOTIFICATION_SENT,
        target.NOTIFICATION_READ = src.NOTIFICATION_READ,
        target.CREATED_AT        = src.CREATED_AT,
        target.UPDATED_AT        = src.UPDATED_AT
    WHEN NOT MATCHED THEN INSERT (
        USER_ID, FIRST_NAME, LAST_NAME, MIDDLE_NAME, EMAIL, PASSWORD_HASH, STATUS,
        ADDRESS_ID, COUNTRY, STATE, DISTRICT, CITY, VILLAGE, ZIPCODE, ADDRESS_TYPE,
        MOBILE_ID, COUNTRY_CODE, PHONE_NUMBER, IS_PRIMARY, ROLE_ID, ROLE_NAME,
        DEVICE_NAME, BROWSER, IP_ADDRESS, LOGIN_STATUS, FULL_DATE, DAY, MONTH,
        QUARTER, YEAR, DAY_NAME, HOUR, MINUTE, SECOND, ACTIVITY_NAME, ACTIVITY_COUNT,
        NOTIFICATION_TYPE, NOTIFICATION_SENT, NOTIFICATION_READ, CREATED_AT, UPDATED_AT
    ) VALUES (
        src.USER_ID, src.FIRST_NAME, src.LAST_NAME, src.MIDDLE_NAME, src.EMAIL, src.PASSWORD_HASH, src.STATUS,
        src.ADDRESS_ID, src.COUNTRY, src.STATE, src.DISTRICT, src.CITY, src.VILLAGE, src.ZIPCODE, src.ADDRESS_TYPE,
        src.MOBILE_ID, src.COUNTRY_CODE, src.PHONE_NUMBER, src.IS_PRIMARY, src.ROLE_ID, src.ROLE_NAME,
        src.DEVICE_NAME, src.BROWSER, src.IP_ADDRESS, src.LOGIN_STATUS, src.FULL_DATE, src.DAY, src.MONTH,
        src.QUARTER, src.YEAR, src.DAY_NAME, src.HOUR, src.MINUTE, src.SECOND, src.ACTIVITY_NAME, src.ACTIVITY_COUNT,
        src.NOTIFICATION_TYPE, src.NOTIFICATION_SENT, src.NOTIFICATION_READ, src.CREATED_AT, src.UPDATED_AT
    );
    """
    
    session.sql(merge_sql).collect()
    
    return f"Successfully processed {len(df)} records ({len(df_clean)} Merged into CLEAN, {len(df_quarantine)} sent to QUARANTINE)."

    $$;




CREATE OR REPLACE PROCEDURE ecommerce.clean.CLEAN_SELLER_PROCEDURE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'pyarrow',
    'numpy'
)
HANDLER = 'run_seller_model_etl'
AS
$$
import re
import pandas as pd
import numpy as np

def run_seller_model_etl(session):
    # ---------------------------------------------------------
    # 1. READ RAW DATA (From Table or External Stage)
    # ---------------------------------------------------------
    # Option A: Read from RAW Table
    raw_df = session.table("ECOMMERCE.RAW.SELLER_MODEL").to_pandas()
    
    # Option B: (Alternative) Read from External Stage if needed
    # session.sql("USE SCHEMA ECOMMERCE_DW.RAW_STAGES").collect()
    # df_stage = session.read.format("csv").option("SKIP_HEADER", 1).load("@ECOMMERCE_DW.RAW_STAGES.EXTERNAL_STAGE_Sellers")
    # raw_df = df_stage.to_pandas()

    if raw_df.empty:
        return "No data found in RAW seller table."
        
    df = raw_df.copy()
    
    # Strip leading/trailing whitespace across all text columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Track error reasons per row for quarantine
    df['ERROR_REASONS'] = [[] for _ in range(len(df))]

    # Helper function to append error reasons
    def add_error(mask, reason_msg):
        for idx in df[mask].index:
            df.loc[idx, 'ERROR_REASONS'].append(reason_msg)

    # ---------------------------------------------------------
    # 2. SOFT CLEANING & REPAIRS (Clean formatted text & Sentinel values)
    # ---------------------------------------------------------
    
    # 🧹 Convert Sentinel Values to NULL (USER_ID = 999999 -> NULL, ROLE_ID = -1 -> NULL)
    user_id_num = pd.to_numeric(df["USER_ID"], errors="coerce")
    df["USER_ID"] = np.where(user_id_num == 999999, np.nan, user_id_num)

    role_id_num = pd.to_numeric(df["ROLE_ID"], errors="coerce")
    df["ROLE_ID"] = np.where(role_id_num == -1, np.nan, role_id_num)

    # 🧹 QUANTITY Cleaning: e.g., '50 units' -> 50
    df["QUANTITY"] = (
        df["QUANTITY"]
        .astype(str)
        .str.extract(r"([-+]?\d*\.?\d+)", expand=False)
    )
    df["QUANTITY"] = pd.to_numeric(df["QUANTITY"], errors="coerce")

    # 🧹 ITEM_SELLING_PRICE Cleaning: e.g., '$442.4' or 'Rs. 1866' -> 442.4, 1866
    df["ITEM_SELLING_PRICE"] = (
        df["ITEM_SELLING_PRICE"]
        .astype(str)
        .str.extract(r"([-+]?\d*\.?\d+)", expand=False)
    )
    df["ITEM_SELLING_PRICE"] = pd.to_numeric(df["ITEM_SELLING_PRICE"], errors="coerce")

    # 🧹 DISCOUNT_PERCENTAGE Cleaning: e.g., '19%' -> 19
    df["DISCOUNT_PERCENTAGE"] = (
        df["DISCOUNT_PERCENTAGE"]
        .astype(str)
        .str.extract(r"([-+]?\d*\.?\d+)", expand=False)
    )
    df["DISCOUNT_PERCENTAGE"] = pd.to_numeric(df["DISCOUNT_PERCENTAGE"], errors="coerce")

    # Note: GST_NUMBER, PAN, BUSINESS_NAME, BUSINESS_EMAIL are kept as-is (NULLs allowed)

    # ---------------------------------------------------------
    # 3. HARD VALIDATION CHECKS (Failures -> Quarantine)
    # ---------------------------------------------------------
    
    # Check 1: SELLER_ID IS NULL or non-numeric
    seller_id_num = pd.to_numeric(df["SELLER_ID"], errors="coerce")
    add_error(seller_id_num.isna(), "SELLER_ID is NULL or non-numeric")

    # Check 2: SELLER_PRODUCT_ID IS NULL or non-numeric
    sp_id_num = pd.to_numeric(df["SELLER_PRODUCT_ID"], errors="coerce")
    add_error(sp_id_num.isna(), "SELLER_PRODUCT_ID is NULL or non-numeric")

    # Check 3: PRODUCT_ID IS NULL or 'INVALID_PROD'
    invalid_product = (
        df["PRODUCT_ID"].isna() 
        | (df["PRODUCT_ID"].astype(str).str.upper() == "INVALID_PROD") 
        | (df["PRODUCT_ID"].astype(str).str.strip() == "")
    )
    add_error(invalid_product, "PRODUCT_ID is NULL, empty, or INVALID_PROD")

    # Check 4: QUANTITY < 0 or NULL after extraction
    invalid_qty = df["QUANTITY"].isna() | (df["QUANTITY"] < 0)
    add_error(invalid_qty, "QUANTITY is missing or negative (< 0)")

    # Check 5: ITEM_SELLING_PRICE < 0 or NULL after extraction
    invalid_price = df["ITEM_SELLING_PRICE"].isna() | (df["ITEM_SELLING_PRICE"] < 0)
    add_error(invalid_price, "ITEM_SELLING_PRICE is missing or negative (< 0)")

    # Check 6: DISCOUNT_PERCENTAGE < 0 OR > 100 or NULL after extraction
    invalid_discount = (
        df["DISCOUNT_PERCENTAGE"].isna() 
        | (df["DISCOUNT_PERCENTAGE"] < 0) 
        | (df["DISCOUNT_PERCENTAGE"] > 100)
    )
    add_error(invalid_discount, "DISCOUNT_PERCENTAGE is invalid (< 0 or > 100)")

    # ---------------------------------------------------------
    # 4. SPLIT DATA: CLEAN vs QUARANTINE
    # ---------------------------------------------------------
    has_errors_mask = df["ERROR_REASONS"].apply(lambda errs: len(errs) > 0)

    # Quarantine records
    df_quarantine = df[has_errors_mask].copy()
    if not df_quarantine.empty:
        df_quarantine["ERROR_REASON"] = df_quarantine["ERROR_REASONS"].apply(lambda errs: "; ".join(errs))
        quarantine_to_write = df_quarantine[[
            "SELLER_ID", "SELLER_PRODUCT_ID", "PRODUCT_ID", 
            "BUSINESS_NAME", "BUSINESS_EMAIL", "ERROR_REASON"
        ]].copy()
        
        session.write_pandas(
            quarantine_to_write,
            table_name="SELLER_MODEL",
            database="ECOMMERCE",
            schema="QUARANTINE",
            auto_create_table=False,
            overwrite=False
        )

    # Clean records
    df_clean = df[~has_errors_mask].copy()
    if df_clean.empty:
        return f"All records quarantined ({len(df_quarantine)} sent to QUARANTINE). No valid data to merge."

    # ---------------------------------------------------------
    # 5. ENFORCE STRICT DATA TYPES ON CLEAN DATAFRAME
    # ---------------------------------------------------------
    int_cols = ["SELLER_ID", "USER_ID", "ROLE_ID", "SELLER_PRODUCT_ID", "QUANTITY"]
    for col in int_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Int64")

    float_cols = ["ITEM_SELLING_PRICE", "DISCOUNT_PERCENTAGE"]
    for col in float_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Float64")

    df_clean = df_clean.drop(columns=["ERROR_REASONS"])

    # ---------------------------------------------------------
    # 6. WRITE TO STAGING & EXECUTE MERGE
    # ---------------------------------------------------------
    session.sql("TRUNCATE TABLE ecommerce.temp.TEMP_CLEAN_SELLER_MODEL").collect()
    session.write_pandas(
        df_clean,
        table_name="TEMP_CLEAN_SELLER_MODEL",
        database="ECOMMERCE",
        schema="TEMP",
        auto_create_table=False,
        overwrite=True
    )

    # Merge into production CLEAN table
    merge_sql = """
    MERGE INTO ecommerce.CLEAN.CLEAN_SELLER_MODEL target
    USING ecommerce.TEMP.TEMP_CLEAN_SELLER_MODEL src
    ON target.SELLER_ID = src.SELLER_ID AND target.SELLER_PRODUCT_ID = src.SELLER_PRODUCT_ID
    WHEN MATCHED THEN UPDATE SET
        target.GST_NUMBER          = src.GST_NUMBER,
        target.PAN                 = src.PAN,
        target.BUSINESS_NAME       = src.BUSINESS_NAME,
        target.BUSINESS_EMAIL      = src.BUSINESS_EMAIL,
        target.USER_ID             = src.USER_ID,
        target.ROLE_ID             = src.ROLE_ID,
        target.PRODUCT_ID          = src.PRODUCT_ID,
        target.ITEM_SELLING_PRICE  = src.ITEM_SELLING_PRICE,
        target.QUANTITY            = src.QUANTITY,
        target.DISCOUNT_PERCENTAGE = src.DISCOUNT_PERCENTAGE
    WHEN NOT MATCHED THEN INSERT (
        SELLER_ID, GST_NUMBER, PAN, BUSINESS_NAME, BUSINESS_EMAIL, USER_ID, ROLE_ID,
        SELLER_PRODUCT_ID, PRODUCT_ID, ITEM_SELLING_PRICE, QUANTITY, DISCOUNT_PERCENTAGE
    ) VALUES (
        src.SELLER_ID, src.GST_NUMBER, src.PAN, src.BUSINESS_NAME, src.BUSINESS_EMAIL, src.USER_ID, src.ROLE_ID,
        src.SELLER_PRODUCT_ID, src.PRODUCT_ID, src.ITEM_SELLING_PRICE, src.QUANTITY, src.DISCOUNT_PERCENTAGE
    );
    """
    
    session.sql(merge_sql).collect()
    
    return f"Successfully processed {len(df)} records ({len(df_clean)} Merged into CLEAN, {len(df_quarantine)} sent to QUARANTINE)."
$$;



CREATE OR REPLACE PROCEDURE ecommerce.clean.CLEAN_PRODUCT_PROCEDURE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'pyarrow',
    'numpy'
)
HANDLER = 'run_product_catalog_etl'
AS
$$
import re
import pandas as pd
import numpy as np

def run_product_catalog_etl(session):
    # ---------------------------------------------------------
    # 1. READ RAW DATA (Table or External Stage)
    # ---------------------------------------------------------
    raw_df = session.table("ECOMMERCE.RAW.PRODUCT_MODEL").to_pandas()
    
    # Alternative: Read directly from CSV stage
    # session.sql("USE SCHEMA ECOMMERCE_DW.RAW_STAGES").collect()
    # raw_df = session.read.format("csv").option("SKIP_HEADER", 1).load("@ECOMMERCE_DW.RAW_STAGES.EXTERNAL_STAGE_Products").to_pandas()

    if raw_df.empty:
        return "No data found in RAW table."
        
    df = raw_df.copy()
    
    # Strip leading/trailing whitespace across all string columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Track error reasons per row for Quarantine
    df['ERROR_REASONS'] = [[] for _ in range(len(df))]

    def add_error(mask, reason_msg):
        for idx in df[mask].index:
            df.loc[idx, 'ERROR_REASONS'].append(reason_msg)

    # ---------------------------------------------------------
    # 2. SOFT CLEANING & REPAIRS (Fix attributes without deleting)
    # ---------------------------------------------------------
    
    # 🟡 FIX: Invalid/Sentinel BRAND_ID (999 -> NULL)
    brand_id_num = pd.to_numeric(df["BRAND_ID"], errors="coerce")
    df["BRAND_ID"] = np.where(brand_id_num == 999, np.nan, brand_id_num)

    # 🟡 FIX: Duplicate SKU (Nullify duplicates so row is preserved)
    df["SKU"] = df["SKU"].astype(str).str.strip()
    df.loc[df["SKU"].isin(["nan", "None", "", "NULL", "null"]), "SKU"] = np.nan
    df.loc[df["SKU"].duplicated(keep=False) & df["SKU"].notna(), "SKU"] = np.nan

    # 🟡 FIX: Recover NULL PRODUCT_NAME (Fallback to MODEL or 'UNKNOWN_PRODUCT')
    df["PRODUCT_NAME"] = df["PRODUCT_NAME"].astype(str).str.strip()
    df.loc[df["PRODUCT_NAME"].isin(["nan", "None", "", "NULL", "null"]), "PRODUCT_NAME"] = np.nan
    df["PRODUCT_NAME"] = df["PRODUCT_NAME"].fillna(df["MODEL"]).fillna("UNKNOWN_PRODUCT")

    # 🟡 FIX: Formatting in PRICE (Strip $, ₹, Rs., commas -> numeric)
    df["PRODUCT_MARKET_PRICE"] = (
        df["PRODUCT_MARKET_PRICE"]
        .astype(str)
        .str.replace(r"[$₹Rs.,]", "", regex=True)
        .str.strip()
    )
    df["PRODUCT_MARKET_PRICE"] = pd.to_numeric(df["PRODUCT_MARKET_PRICE"], errors="coerce")

    # 🟡 FIX: Unit Normalization for WEIGHT (Convert grams 'g' -> 'kg')
    weight_str = df["WEIGHT"].astype(str).str.lower().str.strip()
    is_kg = weight_str.str.contains("kg", regex=False)
    is_gram = weight_str.str.contains("g", regex=False) & ~is_kg
    
    extracted_weight = weight_str.str.extract(r"(\d*\.?\d+)", expand=False).astype(float)
    df["WEIGHT"] = np.where(
        is_gram, extracted_weight / 1000.0,
        np.where(is_kg, extracted_weight, extracted_weight)
    )
    df["WEIGHT"] = pd.to_numeric(df["WEIGHT"], errors="coerce")

    # 🟡 FIX: Unit Normalization for DIMENSIONS (Convert inches & mm -> cm)
    def normalize_dimensions(val):
        if pd.isna(val) or str(val).strip().lower() in ["none", "nan", "", "null"]:
            return None
        v = str(val).lower().strip()
        match = re.search(r"([\d.]+)\s*x\s*([\d.]+)\s*x\s*([\d.]+)", v)
        if not match:
            return v.replace("cm", "").strip()
        
        l, w, h = float(match.group(1)), float(match.group(2)), float(match.group(3))
        if "inch" in v or "in" in v:
            l, w, h = l * 2.54, w * 2.54, h * 2.54
        elif "mm" in v:
            l, w, h = l / 10.0, w / 10.0, h / 10.0
            
        return f"{round(l, 2)}x{round(w, 2)}x{round(h, 2)}"

    df["DIMENSIONS"] = df["DIMENSIONS"].apply(normalize_dimensions)

    # 🟡 FIX: BRAND standardization (Uppercase & Trim)
    df["BRAND_NAME"] = df["BRAND_NAME"].astype(str).str.strip().str.upper()
    df.loc[df["BRAND_NAME"].isin(["NAN", "NONE", "NULL", ""]), "BRAND_NAME"] = np.nan

    # 🟡 FIX: CATEGORY standardization (Uppercase & replace 'SHOES'/'SHOE' -> 'FOOTWEAR')
    df["CATEGORY_NAME"] = df["CATEGORY_NAME"].astype(str).str.strip().str.upper()
    df["CATEGORY_NAME"] = df["CATEGORY_NAME"].replace({"SHOES": "FOOTWEAR", "SHOE": "FOOTWEAR"})
    df.loc[df["CATEGORY_NAME"].isin(["NAN", "NONE", "NULL", ""]), "CATEGORY_NAME"] = np.nan

    # Boolean IS_PRIMARY conversion
    primary_true = df["IS_PRIMARY"].astype(str).str.upper().isin(["Y", "YES", "TRUE", "1"])
    df["IS_PRIMARY"] = np.where(primary_true, True, False)

    # 🟢 KEEP Optional Nullable Fields: MODEL, COLOR, SIZE, STORAGE, IMAGE_ID, IMAGE_URL, DESCRIPTION

    # ---------------------------------------------------------
    # 3. HARD VALIDATION CHECKS (Failures -> Quarantine)
    # ---------------------------------------------------------
    
    # 🔴 DELETE Check 1: PRODUCT_ID IS NULL / non-numeric / 'INVALID_PROD'
    prod_id_num = pd.to_numeric(df["PRODUCT_ID"], errors="coerce")
    invalid_product_id = (
        prod_id_num.isna() 
        | (df["PRODUCT_ID"].astype(str).str.upper() == "INVALID_PROD")
        | (df["PRODUCT_ID"].astype(str).str.strip() == "")
    )
    add_error(invalid_product_id, "PRODUCT_ID is NULL, non-numeric, or INVALID_PROD")

    # 🔴 DELETE Check 2: VARIANT_ID IS NULL or non-numeric
    var_id_num = pd.to_numeric(df["VARIANT_ID"], errors="coerce")
    add_error(var_id_num.isna(), "VARIANT_ID is NULL or non-numeric")

    # 🔴 DELETE Check 3: Negative Price
    add_error(df["PRODUCT_MARKET_PRICE"] < 0, "PRODUCT_MARKET_PRICE is negative")

    # ---------------------------------------------------------
    # 4. SPLIT DATA: CLEAN vs QUARANTINE
    # ---------------------------------------------------------
    has_errors_mask = df["ERROR_REASONS"].apply(lambda errs: len(errs) > 0)

    # Prepare Quarantine Dataframe
    df_quarantine = df[has_errors_mask].copy()
    if not df_quarantine.empty:
        df_quarantine["ERROR_REASON"] = df_quarantine["ERROR_REASONS"].apply(lambda errs: "; ".join(errs))
        quarantine_to_write = df_quarantine[[
            "PRODUCT_ID", "VARIANT_ID", "PRODUCT_NAME", "BRAND_NAME", "SKU", "ERROR_REASON"
        ]].copy()
        
        session.write_pandas(
            quarantine_to_write,
            table_name="PRODUCT_MODEL",
            database="ECOMMERCE",
            schema="QUARANTINE",
            auto_create_table=False,
            overwrite=False
        )

    # Prepare Clean Dataframe
    df_clean = df[~has_errors_mask].copy()
    if df_clean.empty:
        return f"All records quarantined ({len(df_quarantine)} sent to QUARANTINE). No valid data to merge."

    # ---------------------------------------------------------
    # 5. ENFORCE DATA TYPES & TIMESTAMP FORMATTING
    # ---------------------------------------------------------
    int_cols = ["BRAND_ID", "CATEGORY_ID", "PRODUCT_ID", "VARIANT_ID", "IMAGE_ID"]
    for col in int_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Int64")

    float_cols = ["PRODUCT_MARKET_PRICE", "WEIGHT"]
    for col in float_cols:
        df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Float64")

    df_clean["CREATED_AT"] = pd.to_datetime(df_clean["CREATED_AT"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
    df_clean["UPLOADED_AT"] = pd.to_datetime(df_clean["UPLOADED_AT"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

    df_clean = df_clean.drop(columns=["ERROR_REASONS"])

    # ---------------------------------------------------------
    # 6. WRITE TO STAGING & EXECUTE MERGE
    # ---------------------------------------------------------
    session.sql("TRUNCATE TABLE ecommerce.temp.TEMP_CLEAN_PRODUCT_MODEL").collect()
    session.write_pandas(
        df_clean,
        table_name="TEMP_CLEAN_PRODUCT_MODEL",
        database="ECOMMERCE",
        schema="TEMP",
        auto_create_table=False,
        overwrite=True
    )

    # MERGE Statement into Production CLEAN Table
    merge_sql = """
    MERGE INTO ecommerce.CLEAN.CLEAN_PRODUCT_MODEL target
    USING ecommerce.TEMP.TEMP_CLEAN_PRODUCT_MODEL src
    ON target.PRODUCT_ID = src.PRODUCT_ID AND target.VARIANT_ID = src.VARIANT_ID
    WHEN MATCHED THEN UPDATE SET
        target.BRAND_ID             = src.BRAND_ID,
        target.BRAND_NAME           = src.BRAND_NAME,
        target.CATEGORY_ID          = src.CATEGORY_ID,
        target.CATEGORY_NAME        = src.CATEGORY_NAME,
        target.PRODUCT_NAME         = src.PRODUCT_NAME,
        target.MODEL                = src.MODEL,
        target.PRODUCT_MARKET_PRICE = src.PRODUCT_MARKET_PRICE,
        target.DESCRIPTION          = src.DESCRIPTION,
        target.WEIGHT               = src.WEIGHT,
        target.DIMENSIONS           = src.DIMENSIONS,
        target.CREATED_AT           = src.CREATED_AT,
        target.IMAGE_ID             = src.IMAGE_ID,
        target.IMAGE_URL            = src.IMAGE_URL,
        target.IS_PRIMARY           = src.IS_PRIMARY,
        target.UPLOADED_AT          = src.UPLOADED_AT,
        target.COLOR                = src.COLOR,
        target.SIZE                 = src.SIZE,
        target.STORAGE              = src.STORAGE,
        target.SKU                  = src.SKU
    WHEN NOT MATCHED THEN INSERT (
        BRAND_ID, BRAND_NAME, CATEGORY_ID, CATEGORY_NAME, PRODUCT_ID, PRODUCT_NAME,
        MODEL, PRODUCT_MARKET_PRICE, DESCRIPTION, WEIGHT, DIMENSIONS, CREATED_AT,
        IMAGE_ID, IMAGE_URL, IS_PRIMARY, UPLOADED_AT, VARIANT_ID, COLOR, SIZE, STORAGE, SKU
    ) VALUES (
        src.BRAND_ID, src.BRAND_NAME, src.CATEGORY_ID, src.CATEGORY_NAME, src.PRODUCT_ID, src.PRODUCT_NAME,
        src.MODEL, src.PRODUCT_MARKET_PRICE, src.DESCRIPTION, src.WEIGHT, src.DIMENSIONS, src.CREATED_AT,
        src.IMAGE_ID, src.IMAGE_URL, src.IS_PRIMARY, src.UPLOADED_AT, src.VARIANT_ID, src.COLOR, src.SIZE, src.STORAGE, src.SKU
    );
    """
    
    session.sql(merge_sql).collect()
    
    return f"Successfully processed {len(df)} records ({len(df_clean)} Merged into CLEAN, {len(df_quarantine)} sent to QUARANTINE)."
$$;



CREATE OR REPLACE PROCEDURE ecommerce.clean.CLEAN_SHIPMENT_PROCEDURE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'pyarrow',
    'numpy'
)
HANDLER = 'run_shipment_model_etl'
AS
$$
import re
import pandas as pd
import numpy as np

def run_shipment_model_etl(session):
    # ---------------------------------------------------------
    # 1. READ RAW DATA
    # ---------------------------------------------------------
    raw_df = session.table("ECOMMERCE.RAW.SHIPMENT_MODEL").to_pandas()
    
    if raw_df.empty:
        return "No data found in RAW shipment table."
        
    df = raw_df.copy()
    
    # Strip leading/trailing whitespace across all text columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Track error reasons per row for quarantine
    df['ERROR_REASONS'] = [[] for _ in range(len(df))]

    # Helper function to append error reasons
    def add_error(mask, reason_msg):
        for idx in df[mask].index:
            df.loc[idx, 'ERROR_REASONS'].append(reason_msg)

    # ---------------------------------------------------------
    # 2. HARD VALIDATION CHECKS (Missing or Duplicate SHIPMENT_ID)
    # ---------------------------------------------------------
    
    # Check 1: Missing or non-numeric SHIPMENT_ID
    shipment_id_num = pd.to_numeric(df["SHIPMENT_ID"], errors="coerce")
    add_error(shipment_id_num.isna(), "SHIPMENT_ID is NULL or non-numeric")

    # Check 2: Duplicate SHIPMENT_ID in batch
    dup_shipment_ids = shipment_id_num.duplicated(keep=False) & shipment_id_num.notna()
    add_error(dup_shipment_ids, "Duplicate SHIPMENT_ID in batch")

    # ---------------------------------------------------------
    # 3. SOFT CLEANING & REPAIRS (Convert invalid/placeholders to NULL)
    # ---------------------------------------------------------
    
    # Numeric conversions for IDs
    df["ORDER_ID"] = pd.to_numeric(df["ORDER_ID"], errors="coerce")
    df["TRACKING_ID"] = pd.to_numeric(df["TRACKING_ID"], errors="coerce")

    # SHIPPING_CHARGE Cleaning ($ / Rs. removal, FREE -> 0, Negative -> NULL)
    shipping_charge = (
        df["SHIPPING_CHARGE"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace("Rs.", "", regex=False)
        .str.strip()
    )
    shipping_charge = shipping_charge.replace({"FREE": "0", "free": "0", "Free": "0"})
    shipping_charge_num = pd.to_numeric(shipping_charge, errors="coerce")
    df["SHIPPING_CHARGE"] = np.where(shipping_charge_num < 0, None, shipping_charge_num)

    # STATUS & SHIPMENT_STATUS Cleaning (INVALID_STATUS, UNKNOWN, '-' -> NULL)
    for status_col in ["SHIPMENT_STATUS", "STATUS"]:
        if status_col in df.columns:
            cleaned_status = df[status_col].astype(str).str.strip().str.upper()
            df[status_col] = cleaned_status.replace({
                "INVALID_STATUS": None,
                "UNKNOWN": None,
                "-": None,
                "NAN": None,
                "NONE": None
            })

    # LOCATION Cleaning (UNKNOWN_LOCATION, UNKNOWN, '-' -> NULL)
    if "LOCATION" in df.columns:
        cleaned_loc = df["LOCATION"].astype(str).str.strip().str.upper()
        df["LOCATION"] = cleaned_loc.replace({
            "UNKNOWN_LOCATION": None,
            "UNKNOWN": None,
            "-": None,
            "NAN": None,
            "NONE": None
        })

    # COURIER_NAME Cleaning (UNKNOWN, '-' -> NULL)
    if "COURIER_NAME" in df.columns:
        cleaned_courier = df["COURIER_NAME"].astype(str).str.strip().str.upper()
        df["COURIER_NAME"] = cleaned_courier.replace({
            "UNKNOWN": None,
            "-": None,
            "NAN": None,
            "NONE": None
        })

    # TRACKING_NUMBER Cleaning (UNKNOWN, TRACKING_PENDING, '-' -> NULL)
    if "TRACKING_NUMBER" in df.columns:
        cleaned_tn = df["TRACKING_NUMBER"].astype(str).str.strip().str.upper()
        df["TRACKING_NUMBER"] = cleaned_tn.replace({
            "UNKNOWN": None,
            "TRACKING_PENDING": None,
            "-": None,
            "NAN": None,
            "NONE": None
        })

    # DATES & TIMESTAMPS Normalization & Validation
    shipped_at_dt = pd.to_datetime(df["SHIPPED_AT"], errors="coerce")
    delivered_at_dt = pd.to_datetime(df["DELIVERED_AT"], errors="coerce")
    expected_delivery_dt = pd.to_datetime(df["EXPECTED_DELIVERY_DATE"], errors="coerce", dayfirst=True)
    updated_at_dt = pd.to_datetime(df["UPDATED_AT"], errors="coerce")

    # Date Logic Rule 1: DELIVERED_AT cannot be before SHIPPED_AT
    invalid_delivery = delivered_at_dt < shipped_at_dt
    delivered_at_dt = np.where(invalid_delivery, pd.NaT, delivered_at_dt)

    # Date Logic Rule 2: EXPECTED_DELIVERY_DATE cannot be before SHIPPED_AT date
    invalid_expected = expected_delivery_dt < shipped_at_dt.dt.normalize()
    expected_delivery_dt = np.where(invalid_expected, pd.NaT, expected_delivery_dt)

    df["SHIPPED_AT"] = shipped_at_dt
    df["DELIVERED_AT"] = pd.to_datetime(delivered_at_dt)
    df["EXPECTED_DELIVERY_DATE"] = pd.to_datetime(expected_delivery_dt)
    df["UPDATED_AT"] = updated_at_dt

    # ---------------------------------------------------------
    # 4. SPLIT DATA: CLEAN vs QUARANTINE
    # ---------------------------------------------------------
    has_errors_mask = df["ERROR_REASONS"].apply(lambda errs: len(errs) > 0)

    # Quarantine records (Missing or Duplicate SHIPMENT_ID)
    df_quarantine = df[has_errors_mask].copy()
    if not df_quarantine.empty:
        df_quarantine["ERROR_REASON"] = df_quarantine["ERROR_REASONS"].apply(lambda errs: "; ".join(errs))
        quarantine_cols = [c for c in ["SHIPMENT_ID", "ORDER_ID", "TRACKING_NUMBER", "ERROR_REASON"] if c in df_quarantine.columns]
        quarantine_to_write = df_quarantine[quarantine_cols].copy()
        
        session.write_pandas(
            quarantine_to_write,
            table_name="SHIPMENT_MODEL",
            database="ECOMMERCE",
            schema="QUARANTINE",
            auto_create_table=False,
            overwrite=False
        )

    # Clean records
    df_clean = df[~has_errors_mask].copy()
    if df_clean.empty:
        return f"All records quarantined ({len(df_quarantine)} sent to QUARANTINE). No valid data to merge."

    # ---------------------------------------------------------
    # 5. ENFORCE DATA TYPES & TIMESTAMP FORMATS
    # ---------------------------------------------------------
    int_cols = ["SHIPMENT_ID", "ORDER_ID", "TRACKING_ID"]
    for col in int_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Int64")

    if "SHIPPING_CHARGE" in df_clean.columns:
        df_clean["SHIPPING_CHARGE"] = pd.to_numeric(df_clean["SHIPPING_CHARGE"], errors="coerce").astype("Float64")

    # Format Timestamps for Snowflake
    ts_cols = ["SHIPPED_AT", "DELIVERED_AT", "UPDATED_AT"]
    for col in ts_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    if "EXPECTED_DELIVERY_DATE" in df_clean.columns:
        df_clean["EXPECTED_DELIVERY_DATE"] = df_clean["EXPECTED_DELIVERY_DATE"].dt.strftime("%Y-%m-%d")

    df_clean = df_clean.drop(columns=["ERROR_REASONS"])

    # ---------------------------------------------------------
    # 6. WRITE TO STAGING & EXECUTE MERGE
    # ---------------------------------------------------------
    session.sql("TRUNCATE TABLE ecommerce.temp.TEMP_CLEAN_SHIPMENT_MODEL").collect()
    session.write_pandas(
        df_clean,
        table_name="TEMP_CLEAN_SHIPMENT_MODEL",
        database="ECOMMERCE",
        schema="TEMP",
        auto_create_table=False,
        overwrite=True
    )

    merge_sql = """
    MERGE INTO ecommerce.CLEAN.CLEAN_SHIPMENT_MODEL target
    USING ecommerce.TEMP.TEMP_CLEAN_SHIPMENT_MODEL src
    ON target.SHIPMENT_ID = src.SHIPMENT_ID
    WHEN MATCHED THEN UPDATE SET
        target.ORDER_ID               = src.ORDER_ID,
        target.TRACKING_ID            = src.TRACKING_ID,
        target.TRACKING_NUMBER        = src.TRACKING_NUMBER,
        target.COURIER_NAME           = src.COURIER_NAME,
        target.LOCATION               = src.LOCATION,
        target.STATUS                 = src.STATUS,
        target.SHIPPING_CHARGE        = src.SHIPPING_CHARGE,
        target.SHIPPED_AT             = src.SHIPPED_AT,
        target.DELIVERED_AT           = src.DELIVERED_AT,
        target.EXPECTED_DELIVERY_DATE = src.EXPECTED_DELIVERY_DATE,
        target.UPDATED_AT             = src.UPDATED_AT
    WHEN NOT MATCHED THEN INSERT (
        SHIPMENT_ID, ORDER_ID, TRACKING_ID, TRACKING_NUMBER, COURIER_NAME, LOCATION,
        STATUS, SHIPPING_CHARGE, SHIPPED_AT, DELIVERED_AT, EXPECTED_DELIVERY_DATE, UPDATED_AT
    ) VALUES (
        src.SHIPMENT_ID, src.ORDER_ID, src.TRACKING_ID, src.TRACKING_NUMBER, src.COURIER_NAME, src.LOCATION,
        src.STATUS, src.SHIPPING_CHARGE, src.SHIPPED_AT, src.DELIVERED_AT, src.EXPECTED_DELIVERY_DATE, src.UPDATED_AT
    );
    """
    
    session.sql(merge_sql).collect()
    
    return f"Successfully processed {len(df)} records ({len(df_clean)} Merged into CLEAN, {len(df_quarantine)} sent to QUARANTINE)."
$$;







CREATE OR REPLACE PROCEDURE ecommerce.clean.CLEAN_RETURN_PROCEDURE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'pyarrow',
    'numpy'
)
HANDLER = 'run_return_model_etl'
AS
$$
import re
import pandas as pd
import numpy as np

def run_return_model_etl(session):
    # ---------------------------------------------------------
    # 1. READ RAW DATA
    # ---------------------------------------------------------
    raw_df = session.table("ECOMMERCE.RAW.RETURN_MODEL").to_pandas()
    
    if raw_df.empty:
        return "No data found in RAW return table."
        
    df = raw_df.copy()
    
    # Strip leading/trailing whitespace across all text columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Track error reasons per row for quarantine
    df['ERROR_REASONS'] = [[] for _ in range(len(df))]

    # Helper function to append error reasons
    def add_error(mask, reason_msg):
        for idx in df[mask].index:
            df.loc[idx, 'ERROR_REASONS'].append(reason_msg)

    # ---------------------------------------------------------
    # 2. HARD VALIDATION CHECKS (Missing or Duplicate RETURN_ID)
    # ---------------------------------------------------------
    
    # Check 1: Missing or non-numeric RETURN_ID
    return_id_num = pd.to_numeric(df["RETURN_ID"], errors="coerce")
    add_error(return_id_num.isna(), "RETURN_ID is NULL or non-numeric")

    # Check 2: Duplicate RETURN_ID in batch
    dup_return_ids = return_id_num.duplicated(keep=False) & return_id_num.notna()
    add_error(dup_return_ids, "Duplicate/conflicting RETURN_ID in batch")

    # ---------------------------------------------------------
    # 3. SOFT CLEANING & NORMALIZATION
    # ---------------------------------------------------------
    
    # Numeric conversions for optional IDs
    df["REFUND_ID"] = pd.to_numeric(df["REFUND_ID"], errors="coerce")
    df["REVIEW_ID"] = pd.to_numeric(df["REVIEW_ID"], errors="coerce")

    # 🟡 REASON Normalization
    if "REASON" in df.columns:
        reason_clean = df["REASON"].astype(str).str.strip().str.upper().str.replace(" ", "_")
        reason_map = {
            "DEFECTIVE_ITEM": "DEFECTIVE",
            "DEFECTIVE": "DEFECTIVE",
            "ITEM_NOT_AS_DESCRIBED": "ITEM_NOT_AS_DESCRIBED",
            "ITEM_NOT_DESCRIBED": "ITEM_NOT_AS_DESCRIBED",
            "WRONG_ITEM": "WRONG_ITEM",
            "DAMAGED": "DAMAGED",
            "CHANGE_OF_MIND": "CHANGE_OF_MIND"
        }
        # Map values or set invalid placeholders (UNKNOWN, '-') to NULL
        df["REASON"] = reason_clean.map(
            lambda x: reason_map.get(x, None if x in ["UNKNOWN", "-", "", "NAN", "NONE"] else x)
        )

    # 🟡 RETURN_STATUS & REFUND_STATUS Normalization (INVALID_STATUS, UNKNOWN, '-' -> NULL)
    for status_col in ["RETURN_STATUS", "REFUND_STATUS"]:
        if status_col in df.columns:
            cleaned_status = df[status_col].astype(str).str.strip().str.upper().str.replace(" ", "_")
            df[status_col] = cleaned_status.replace({
                "INVALID_STATUS": None,
                "UNKNOWN": None,
                "-": None,
                "": None,
                "NAN": None,
                "NONE": None
            })

    # 🟡 REFUND_AMOUNT Cleaning ($ / Rs. removal, Negative -> NULL)
    if "REFUND_AMOUNT" in df.columns:
        refund_str = (
            df["REFUND_AMOUNT"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace("Rs.", "", regex=False)
            .str.strip()
        )
        refund_num = pd.to_numeric(refund_str, errors="coerce")
        df["REFUND_AMOUNT"] = np.where(refund_num < 0, None, refund_num)

    # 🟡 RATING Validation (Must be between 1 and 5, else NULL)
    if "RATING" in df.columns:
        rating_num = pd.to_numeric(df["RATING"], errors="coerce")
        df["RATING"] = np.where(rating_num.between(1, 5), rating_num, None)

    # 🟡 COMMENT Cleaning (UNKNOWN, '-' -> NULL)
    if "COMMENT" in df.columns:
        cleaned_comment = df["COMMENT"].astype(str).str.strip()
        df["COMMENT"] = cleaned_comment.replace({
            "UNKNOWN": None,
            "-": None,
            "": None,
            "NAN": None,
            "NONE": None
        })

    # 🟡 TIMESTAMPS Normalization & Logical Checks
    requested_at_dt = pd.to_datetime(df["REQUESTED_AT"], errors="coerce")
    refunded_at_dt = pd.to_datetime(df["REFUNDED_AT"], errors="coerce")
    reviewed_at_dt = pd.to_datetime(df["REVIEWED_AT"], errors="coerce")

    # Date Logic Check 1: REFUNDED_AT cannot be before REQUESTED_AT
    invalid_refund_date = refunded_at_dt < requested_at_dt
    refunded_at_dt = np.where(invalid_refund_date, pd.NaT, refunded_at_dt)

    # Date Logic Check 2: REVIEWED_AT cannot be before REQUESTED_AT
    invalid_review_date = reviewed_at_dt < requested_at_dt
    reviewed_at_dt = np.where(invalid_review_date, pd.NaT, reviewed_at_dt)

    df["REQUESTED_AT"] = requested_at_dt
    df["REFUNDED_AT"] = pd.to_datetime(refunded_at_dt)
    df["REVIEWED_AT"] = pd.to_datetime(reviewed_at_dt)

    # ---------------------------------------------------------
    # 4. SPLIT DATA: CLEAN vs QUARANTINE
    # ---------------------------------------------------------
    has_errors_mask = df["ERROR_REASONS"].apply(lambda errs: len(errs) > 0)

    # Quarantine records (Missing or Duplicate RETURN_ID)
    df_quarantine = df[has_errors_mask].copy()
    if not df_quarantine.empty:
        df_quarantine["ERROR_REASON"] = df_quarantine["ERROR_REASONS"].apply(lambda errs: "; ".join(errs))
        quarantine_cols = [c for c in ["RETURN_ID", "REFUND_ID", "REVIEW_ID", "ERROR_REASON"] if c in df_quarantine.columns]
        quarantine_to_write = df_quarantine[quarantine_cols].copy()
        
        session.write_pandas(
            quarantine_to_write,
            table_name="RETURN_MODEL",
            database="ECOMMERCE",
            schema="QUARANTINE",
            auto_create_table=False,
            overwrite=False
        )

    # Clean records
    df_clean = df[~has_errors_mask].copy()
    if df_clean.empty:
        return f"All records quarantined ({len(df_quarantine)} sent to QUARANTINE). No valid data to merge."

    # ---------------------------------------------------------
    # 5. ENFORCE DATA TYPES & TIMESTAMP FORMATS
    # ---------------------------------------------------------
    int_cols = ["RETURN_ID", "REFUND_ID", "REVIEW_ID", "RATING"]
    for col in int_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Int64")

    if "REFUND_AMOUNT" in df_clean.columns:
        df_clean["REFUND_AMOUNT"] = pd.to_numeric(df_clean["REFUND_AMOUNT"], errors="coerce").astype("Float64")

    # Format Timestamps for Snowflake
    ts_cols = ["REQUESTED_AT", "REFUNDED_AT", "REVIEWED_AT"]
    for col in ts_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    df_clean = df_clean.drop(columns=["ERROR_REASONS"])

    # ---------------------------------------------------------
    # 6. WRITE TO STAGING & EXECUTE MERGE
    # ---------------------------------------------------------
    session.sql("TRUNCATE TABLE ecommerce.temp.TEMP_CLEAN_RETURN_MODEL").collect()
    session.write_pandas(
        df_clean,
        table_name="TEMP_CLEAN_RETURN_MODEL",
        database="ECOMMERCE",
        schema="TEMP",
        auto_create_table=False,
        overwrite=True
    )

    merge_sql = """
    MERGE INTO ecommerce.CLEAN.CLEAN_RETURN_MODEL target
    USING ecommerce.TEMP.TEMP_CLEAN_RETURN_MODEL src
    ON target.RETURN_ID = src.RETURN_ID
    WHEN MATCHED THEN UPDATE SET
        target.REASON        = src.REASON,
        target.RETURN_STATUS = src.RETURN_STATUS,
        target.REQUESTED_AT  = src.REQUESTED_AT,
        target.REFUND_ID     = src.REFUND_ID,
        target.REFUND_AMOUNT = src.REFUND_AMOUNT,
        target.REFUND_STATUS = src.REFUND_STATUS,
        target.REFUNDED_AT   = src.REFUNDED_AT,
        target.REVIEW_ID     = src.REVIEW_ID,
        target.RATING        = src.RATING,
        target.COMMENT       = src.COMMENT,
        target.REVIEWED_AT   = src.REVIEWED_AT
    WHEN NOT MATCHED THEN INSERT (
        RETURN_ID, REASON, RETURN_STATUS, REQUESTED_AT, REFUND_ID,
        REFUND_AMOUNT, REFUND_STATUS, REFUNDED_AT, REVIEW_ID, RATING, COMMENT, REVIEWED_AT
    ) VALUES (
        src.RETURN_ID, src.REASON, src.RETURN_STATUS, src.REQUESTED_AT, src.REFUND_ID,
        src.REFUND_AMOUNT, src.REFUND_STATUS, src.REFUNDED_AT, src.REVIEW_ID, src.RATING, src.COMMENT, src.REVIEWED_AT
    );
    """
    
    session.sql(merge_sql).collect()
    
    return f"Successfully processed {len(df)} records ({len(df_clean)} Merged into CLEAN, {len(df_quarantine)} sent to QUARANTINE)."
$$;



CREATE OR REPLACE PROCEDURE ecommerce.clean.CLEAN_ORDER_PROCEDURE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'pyarrow',
    'numpy'
)
HANDLER = 'run_order_model_etl'
AS
$$
import re
import pandas as pd
import numpy as np

def run_order_model_etl(session):
    # ---------------------------------------------------------
    # 1. READ RAW DATA
    # ---------------------------------------------------------
    raw_df = session.table("ECOMMERCE.RAW.ORDER_MODEL").to_pandas()
    
    if raw_df.empty:
        return "No data found in RAW order table."
        
    df = raw_df.copy()
    
    # Strip leading/trailing whitespace across all text columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Track error reasons per row for quarantine
    df['ERROR_REASONS'] = [[] for _ in range(len(df))]

    # Helper function to append error reasons
    def add_error(mask, reason_msg):
        for idx in df[mask].index:
            df.loc[idx, 'ERROR_REASONS'].append(reason_msg)

    # ---------------------------------------------------------
    # 2. HARD VALIDATION CHECKS (Missing IDs & Duplicate Items)
    # ---------------------------------------------------------
    
    # Check 1: Missing or non-numeric ORDER_ID
    order_id_num = pd.to_numeric(df["ORDER_ID"], errors="coerce")
    add_error(order_id_num.isna(), "ORDER_ID is NULL or non-numeric")

    # Check 2: Missing or non-numeric ORDER_ITEM_ID
    order_item_id_num = pd.to_numeric(df["ORDER_ITEM_ID"], errors="coerce")
    add_error(order_item_id_num.isna(), "ORDER_ITEM_ID is NULL or non-numeric")

    # Check 3: Duplicate ORDER_ITEM_ID in batch
    dup_item_ids = order_item_id_num.duplicated(keep=False) & order_item_id_num.notna()
    add_error(dup_item_ids, "Duplicate ORDER_ITEM_ID in batch")

    # NOTE: Multiple items can legitimately share the same ORDER_ID (Kept!)

    # ---------------------------------------------------------
    # 3. SOFT CLEANING & REPAIRS (Convert invalid values to NULL)
    # ---------------------------------------------------------
    
    # 🟡 USER_ID: Convert 999999 placeholder to NULL
    user_id_num = pd.to_numeric(df["USER_ID"], errors="coerce")
    df["USER_ID"] = np.where(user_id_num == 999999, np.nan, user_id_num)

    # 🟡 Numeric ID conversions
    for id_col in ["SELLER_PRODUCT_ID", "COUPON_ID"]:
        if id_col in df.columns:
            df[id_col] = pd.to_numeric(df[id_col], errors="coerce")

    # 🟡 QUANTITY Validation: Must be > 0 (e.g. -2, 0 -> NULL)
    if "QUANTITY" in df.columns:
        qty_num = pd.to_numeric(df["QUANTITY"], errors="coerce")
        df["QUANTITY"] = np.where(qty_num > 0, qty_num, None)

    # 🟡 Currency & Financial Fields ($ / Rs. stripping & negative -> NULL)
    currency_cols = [
        "TOTAL_AMOUNT", "UNIT_PRICE", "COST_PRICE", 
        "DISCOUNT_AMOUNT", "TAX_AMOUNT", "SHIPPING_AMOUNT", 
        "REVENUE", "MINIMUM_AMOUNT"
    ]
    for col in currency_cols:
        if col in df.columns:
            cleaned_str = (
                df[col]
                .astype(str)
                .str.replace("$", "", regex=False)
                .str.replace("Rs.", "", regex=False)
                .str.strip()
            )
            cleaned_num = pd.to_numeric(cleaned_str, errors="coerce")
            df[col] = np.where(cleaned_num < 0, None, cleaned_num)

    # 🟡 PROFIT & MARGIN_PERCENT (Allows valid negative profit, strips %)
    for col in ["PROFIT", "MARGIN_PERCENT"]:
        if col in df.columns:
            cleaned_str = df[col].astype(str).str.replace("%", "", regex=False).str.strip()
            df[col] = pd.to_numeric(cleaned_str, errors="coerce")

    # 🟡 DISCOUNT_PERCENTAGE Validation (Must be between 0 and 100)
    if "DISCOUNT_PERCENTAGE" in df.columns:
        disc_pct_str = df["DISCOUNT_PERCENTAGE"].astype(str).str.replace("%", "", regex=False).str.strip()
        disc_pct_num = pd.to_numeric(disc_pct_str, errors="coerce")
        df["DISCOUNT_PERCENTAGE"] = np.where(disc_pct_num.between(0, 100), disc_pct_num, None)

    # 🟡 COUPON_CODE Cleaning (INVALID_CODE, UNKNOWN, '-' -> NULL)
    if "COUPON_CODE" in df.columns:
        coupon_clean = df["COUPON_CODE"].astype(str).str.strip().str.upper()
        df["COUPON_CODE"] = coupon_clean.replace({
            "INVALID_CODE": None,
            "UNKNOWN": None,
            "-": None,
            "": None,
            "NAN": None,
            "NONE": None
        })

    # 🟡 ORDER_STATUS Cleaning (UNKNOWN_STATUS, INVALID_STATUS, UNKNOWN, '-' -> NULL)
    if "ORDER_STATUS" in df.columns:
        status_clean = df["ORDER_STATUS"].astype(str).str.strip().str.upper()
        df["ORDER_STATUS"] = status_clean.replace({
            "UNKNOWN_STATUS": None,
            "INVALID_STATUS": None,
            "UNKNOWN": None,
            "-": None,
            "": None,
            "NAN": None,
            "NONE": None
        })

    # 🟡 IS_ACTIVE Boolean Conversion
    if "IS_ACTIVE" in df.columns:
        active_true = df["IS_ACTIVE"].astype(str).str.upper().isin(["Y", "YES", "TRUE", "1"])
        active_false = df["IS_ACTIVE"].astype(str).str.upper().isin(["N", "NO", "FALSE", "0"])
        df["IS_ACTIVE"] = np.where(active_true, True, np.where(active_false, False, None))

    # 🟡 DATES & TIMESTAMPS Normalization
    for date_col in ["ORDER_DATE", "EXPIRY_DATE"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # ---------------------------------------------------------
    # 4. SPLIT DATA: CLEAN vs QUARANTINE
    # ---------------------------------------------------------
    has_errors_mask = df["ERROR_REASONS"].apply(lambda errs: len(errs) > 0)

    # Quarantine records (Missing ORDER_ID, Missing ORDER_ITEM_ID, or Duplicate ORDER_ITEM_ID)
    df_quarantine = df[has_errors_mask].copy()
    if not df_quarantine.empty:
        df_quarantine["ERROR_REASON"] = df_quarantine["ERROR_REASONS"].apply(lambda errs: "; ".join(errs))
        quarantine_cols = [c for c in ["ORDER_ID", "ORDER_ITEM_ID", "USER_ID", "SELLER_PRODUCT_ID", "ERROR_REASON"] if c in df_quarantine.columns]
        quarantine_to_write = df_quarantine[quarantine_cols].copy()
        
        session.write_pandas(
            quarantine_to_write,
            table_name="ORDER_MODEL",
            database="ECOMMERCE",
            schema="QUARANTINE",
            auto_create_table=False,
            overwrite=False
        )

    # Clean records
    df_clean = df[~has_errors_mask].copy()
    if df_clean.empty:
        return f"All records quarantined ({len(df_quarantine)} sent to QUARANTINE). No valid data to merge."

    # ---------------------------------------------------------
    # 5. ENFORCE DATA TYPES & TIMESTAMP FORMATS
    # ---------------------------------------------------------
    int_cols = ["ORDER_ID", "ORDER_ITEM_ID", "USER_ID", "SELLER_PRODUCT_ID", "QUANTITY", "COUPON_ID"]
    for col in int_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Int64")

    float_cols = [
        "TOTAL_AMOUNT", "UNIT_PRICE", "COST_PRICE", "DISCOUNT_AMOUNT",
        "TAX_AMOUNT", "SHIPPING_AMOUNT", "REVENUE", "PROFIT",
        "MARGIN_PERCENT", "DISCOUNT_PERCENTAGE", "MINIMUM_AMOUNT"
    ]
    for col in float_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Float64")

    # Format Dates/Timestamps for Snowflake
    for date_col in ["ORDER_DATE", "EXPIRY_DATE"]:
        if date_col in df_clean.columns:
            df_clean[date_col] = df_clean[date_col].dt.strftime("%Y-%m-%d %H:%M:%S")

    df_clean = df_clean.drop(columns=["ERROR_REASONS"])

    # ---------------------------------------------------------
    # 6. WRITE TO STAGING & EXECUTE MERGE
    # ---------------------------------------------------------
    session.sql("TRUNCATE TABLE ecommerce.temp.TEMP_CLEAN_ORDER_MODEL").collect()
    session.write_pandas(
        df_clean,
        table_name="TEMP_CLEAN_ORDER_MODEL",
        database="ECOMMERCE",
        schema="TEMP",
        auto_create_table=False,
        overwrite=True
    )

    merge_sql = """
    MERGE INTO ecommerce.CLEAN.CLEAN_ORDER_MODEL target
    USING ecommerce.TEMP.TEMP_CLEAN_ORDER_MODEL src
    ON target.ORDER_ITEM_ID = src.ORDER_ITEM_ID
    WHEN MATCHED THEN UPDATE SET
        target.ORDER_ID            = src.ORDER_ID,
        target.USER_ID             = src.USER_ID,
        target.TOTAL_AMOUNT        = src.TOTAL_AMOUNT,
        target.ORDER_STATUS        = src.ORDER_STATUS,
        target.SELLER_PRODUCT_ID   = src.SELLER_PRODUCT_ID,
        target.QUANTITY            = src.QUANTITY,
        target.UNIT_PRICE          = src.UNIT_PRICE,
        target.COST_PRICE          = src.COST_PRICE,
        target.DISCOUNT_AMOUNT     = src.DISCOUNT_AMOUNT,
        target.TAX_AMOUNT          = src.TAX_AMOUNT,
        target.SHIPPING_AMOUNT     = src.SHIPPING_AMOUNT,
        target.REVENUE             = src.REVENUE,
        target.PROFIT              = src.PROFIT,
        target.MARGIN_PERCENT      = src.MARGIN_PERCENT,
        target.COUPON_ID           = src.COUPON_ID,
        target.COUPON_CODE         = src.COUPON_CODE,
        target.DISCOUNT_PERCENTAGE = src.DISCOUNT_PERCENTAGE,
        target.MINIMUM_AMOUNT      = src.MINIMUM_AMOUNT,
        target.EXPIRY_DATE         = src.EXPIRY_DATE,
        target.IS_ACTIVE           = src.IS_ACTIVE
    WHEN NOT MATCHED THEN INSERT (
        ORDER_ID, USER_ID, TOTAL_AMOUNT, ORDER_STATUS, ORDER_ITEM_ID,
        SELLER_PRODUCT_ID, QUANTITY, UNIT_PRICE, COST_PRICE, DISCOUNT_AMOUNT,
        TAX_AMOUNT, SHIPPING_AMOUNT, REVENUE, PROFIT, MARGIN_PERCENT,
        COUPON_ID, COUPON_CODE, DISCOUNT_PERCENTAGE, MINIMUM_AMOUNT, EXPIRY_DATE, IS_ACTIVE
    ) VALUES (
        src.ORDER_ID, src.USER_ID, src.TOTAL_AMOUNT, src.ORDER_STATUS, src.ORDER_ITEM_ID,
        src.SELLER_PRODUCT_ID, src.QUANTITY, src.UNIT_PRICE, src.COST_PRICE, src.DISCOUNT_AMOUNT,
        src.TAX_AMOUNT, src.SHIPPING_AMOUNT, src.REVENUE, src.PROFIT, src.MARGIN_PERCENT,
        src.COUPON_ID, src.COUPON_CODE, src.DISCOUNT_PERCENTAGE, src.MINIMUM_AMOUNT, src.EXPIRY_DATE, src.IS_ACTIVE
    );
    """
    
    session.sql(merge_sql).collect()
    
    return f"Successfully processed {len(df)} records ({len(df_clean)} Merged into CLEAN, {len(df_quarantine)} sent to QUARANTINE)."
$$;




CREATE OR REPLACE PROCEDURE ecommerce.clean.CLEAN_CART_PROCEDURE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'pyarrow',
    'numpy'
)
HANDLER = 'run_cart_model_etl'
AS
$$
import re
import pandas as pd
import numpy as np

def run_cart_model_etl(session):
    # ---------------------------------------------------------
    # 1. READ RAW DATA
    # ---------------------------------------------------------
    raw_df = session.table("ECOMMERCE.RAW.CART_MODEL").to_pandas()
    
    if raw_df.empty:
        return "No data found in RAW cart table."
        
    df = raw_df.copy()
    
    # Strip leading/trailing whitespace across all text columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Track error reasons per row for quarantine
    df['ERROR_REASONS'] = [[] for _ in range(len(df))]

    # Helper function to append error reasons
    def add_error(mask, reason_msg):
        for idx in df[mask].index:
            df.loc[idx, 'ERROR_REASONS'].append(reason_msg)

    # ---------------------------------------------------------
    # 2. HARD VALIDATION CHECKS (Missing IDs & Duplicate Items)
    # ---------------------------------------------------------
    
    # Check 1: Missing or non-numeric CART_ID
    cart_id_num = pd.to_numeric(df["CART_ID"], errors="coerce")
    add_error(cart_id_num.isna(), "CART_ID is NULL or non-numeric")

    # Check 2: Missing or non-numeric CART_ITEM_ID
    cart_item_id_num = pd.to_numeric(df["CART_ITEM_ID"], errors="coerce")
    add_error(cart_item_id_num.isna(), "CART_ITEM_ID is NULL or non-numeric")

    # Check 3: Duplicate CART_ITEM_ID in batch
    dup_item_ids = cart_item_id_num.duplicated(keep=False) & cart_item_id_num.notna()
    add_error(dup_item_ids, "Duplicate CART_ITEM_ID in batch")

    # NOTE: Multiple cart items can legitimately share the same CART_ID (Kept!)

    # ---------------------------------------------------------
    # 3. SOFT CLEANING & REPAIRS (Convert invalid values to NULL)
    # ---------------------------------------------------------
    
    # 🟡 Optional Numeric IDs (USER_ID, SELLER_PRODUCT_ID, WISHLIST_ID)
    for id_col in ["USER_ID", "SELLER_PRODUCT_ID", "WISHLIST_ID"]:
        if id_col in df.columns:
            df[id_col] = pd.to_numeric(df[id_col], errors="coerce")

    # 🟡 QUANTITY Validation: Must be > 0 (e.g. <= 0 -> NULL)
    if "QUANTITY" in df.columns:
        qty_num = pd.to_numeric(df["QUANTITY"], errors="coerce")
        df["QUANTITY"] = np.where(qty_num > 0, qty_num, None)

    # 🟡 UNIT_PRICE Cleaning ($ / Rs. stripping & negative -> NULL)
    if "UNIT_PRICE" in df.columns:
        cleaned_price_str = (
            df["UNIT_PRICE"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace("Rs.", "", regex=False)
            .str.strip()
        )
        cleaned_price_num = pd.to_numeric(cleaned_price_str, errors="coerce")
        df["UNIT_PRICE"] = np.where(cleaned_price_num < 0, None, cleaned_price_num)

    # 🟡 STATUS Normalization (UPPERCASE & INVALID_STATUS, UNKNOWN_STATUS, '-' -> NULL)
    if "STATUS" in df.columns:
        status_clean = df["STATUS"].astype(str).str.strip().str.upper()
        df["STATUS"] = status_clean.replace({
            "INVALID_STATUS": None,
            "UNKNOWN_STATUS": None,
            "UNKNOWN": None,
            "-": None,
            "": None,
            "NAN": None,
            "NONE": None
        })

    # 🟡 TIMESTAMPS Normalization
    ts_cols = ["CREATED_AT", "UPDATED_AT", "ADDED_AT", "WISHLIST_ADDED_AT"]
    for date_col in ts_cols:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # ---------------------------------------------------------
    # 4. SPLIT DATA: CLEAN vs QUARANTINE
    # ---------------------------------------------------------
    has_errors_mask = df["ERROR_REASONS"].apply(lambda errs: len(errs) > 0)

    # Quarantine records (Missing CART_ID, Missing CART_ITEM_ID, or Duplicate CART_ITEM_ID)
    df_quarantine = df[has_errors_mask].copy()
    if not df_quarantine.empty:
        df_quarantine["ERROR_REASON"] = df_quarantine["ERROR_REASONS"].apply(lambda errs: "; ".join(errs))
        quarantine_cols = [c for c in ["CART_ID", "CART_ITEM_ID", "USER_ID", "SELLER_PRODUCT_ID", "ERROR_REASON"] if c in df_quarantine.columns]
        quarantine_to_write = df_quarantine[quarantine_cols].copy()
        
        session.write_pandas(
            quarantine_to_write,
            table_name="CART_MODEL",
            database="ECOMMERCE",
            schema="QUARANTINE",
            auto_create_table=False,
            overwrite=False
        )

    # Clean records
    df_clean = df[~has_errors_mask].copy()
    if df_clean.empty:
        return f"All records quarantined ({len(df_quarantine)} sent to QUARANTINE). No valid data to merge."

    # ---------------------------------------------------------
    # 5. ENFORCE DATA TYPES & TIMESTAMP FORMATS
    # ---------------------------------------------------------
    int_cols = ["CART_ID", "CART_ITEM_ID", "USER_ID", "SELLER_PRODUCT_ID", "QUANTITY", "WISHLIST_ID"]
    for col in int_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Int64")

    if "UNIT_PRICE" in df_clean.columns:
        df_clean["UNIT_PRICE"] = pd.to_numeric(df_clean["UNIT_PRICE"], errors="coerce").astype("Float64")

    # Format Timestamps for Snowflake
    for date_col in ts_cols:
        if date_col in df_clean.columns:
            df_clean[date_col] = df_clean[date_col].dt.strftime("%Y-%m-%d %H:%M:%S")

    df_clean = df_clean.drop(columns=["ERROR_REASONS"])

    # ---------------------------------------------------------
    # 6. WRITE TO STAGING & EXECUTE MERGE
    # ---------------------------------------------------------
    session.sql("TRUNCATE TABLE ecommerce.temp.TEMP_CLEAN_CART_MODEL").collect()
    session.write_pandas(
        df_clean,
        table_name="TEMP_CLEAN_CART_MODEL",
        database="ECOMMERCE",
        schema="TEMP",
        auto_create_table=False,
        overwrite=True
    )

    merge_sql = """
    MERGE INTO ecommerce.CLEAN.CLEAN_CART_MODEL target
    USING ecommerce.TEMP.TEMP_CLEAN_CART_MODEL src
    ON target.CART_ITEM_ID = src.CART_ITEM_ID
    WHEN MATCHED THEN UPDATE SET
        target.CART_ID           = src.CART_ID,
        target.USER_ID           = src.USER_ID,
        target.SELLER_PRODUCT_ID = src.SELLER_PRODUCT_ID,
        target.QUANTITY          = src.QUANTITY,
        target.UNIT_PRICE        = src.UNIT_PRICE,
        target.STATUS            = src.STATUS,
        target.CREATED_AT        = src.CREATED_AT,
        target.UPDATED_AT        = src.UPDATED_AT,
        target.ADDED_AT          = src.ADDED_AT,
        target.WISHLIST_ID       = src.WISHLIST_ID,
        target.WISHLIST_ADDED_AT = src.WISHLIST_ADDED_AT
    WHEN NOT MATCHED THEN INSERT (
        CART_ID, CART_ITEM_ID, USER_ID, SELLER_PRODUCT_ID, QUANTITY,
        UNIT_PRICE, STATUS, CREATED_AT, UPDATED_AT, ADDED_AT, WISHLIST_ID, WISHLIST_ADDED_AT
    ) VALUES (
        src.CART_ID, src.CART_ITEM_ID, src.USER_ID, src.SELLER_PRODUCT_ID, src.QUANTITY,
        src.UNIT_PRICE, src.STATUS, src.CREATED_AT, src.UPDATED_AT, src.ADDED_AT, src.WISHLIST_ID, src.WISHLIST_ADDED_AT
    );
    """
    
    session.sql(merge_sql).collect()
    
    return f"Successfully processed {len(df)} records ({len(df_clean)} Merged into CLEAN, {len(df_quarantine)} sent to QUARANTINE)."
$$;


CREATE OR REPLACE PROCEDURE ecommerce.clean.CLEAN_PAYMENT_PROCEDURE()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = (
    'snowflake-snowpark-python',
    'pandas',
    'pyarrow',
    'numpy'
)
HANDLER = 'run_payment_model_etl'
AS
$$
import re
import pandas as pd
import numpy as np

def run_payment_model_etl(session):
    # ---------------------------------------------------------
    # 1. READ RAW DATA
    # ---------------------------------------------------------
    raw_df = session.table("ECOMMERCE.RAW.PAYMENT_MODEL").to_pandas()
    
    if raw_df.empty:
        return "No data found in RAW payment table."
        
    df = raw_df.copy()
    
    # Strip leading/trailing whitespace across all text columns
    str_cols = df.select_dtypes(include=['object']).columns
    df[str_cols] = df[str_cols].apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    
    # Track error reasons per row for quarantine
    df['ERROR_REASONS'] = [[] for _ in range(len(df))]

    # Helper function to append error reasons
    def add_error(mask, reason_msg):
        for idx in df[mask].index:
            df.loc[idx, 'ERROR_REASONS'].append(reason_msg)

    # ---------------------------------------------------------
    # 2. HARD VALIDATION CHECKS (Missing or Duplicate PAYMENT_ID)
    # ---------------------------------------------------------
    
    # Check 1: Missing or non-numeric PAYMENT_ID
    payment_id_num = pd.to_numeric(df["PAYMENT_ID"], errors="coerce")
    add_error(payment_id_num.isna(), "PAYMENT_ID is NULL or non-numeric")

    # Check 2: Duplicate PAYMENT_ID in batch
    dup_payment_ids = payment_id_num.duplicated(keep=False) & payment_id_num.notna()
    add_error(dup_payment_ids, "Duplicate PAYMENT_ID in batch")

    # ---------------------------------------------------------
    # 3. SOFT CLEANING & REPAIRS (Convert invalid values to NULL)
    # ---------------------------------------------------------
    
    # 🟡 Optional ORDER_ID Numeric Conversion (Kept if missing)
    if "ORDER_ID" in df.columns:
        df["ORDER_ID"] = pd.to_numeric(df["ORDER_ID"], errors="coerce")

    # 🟡 TRANSACTION_ID Cleaning ('PENDING_TXN_ID', 'UNKNOWN', '-' -> NULL)
    if "TRANSACTION_ID" in df.columns:
        txn_clean = df["TRANSACTION_ID"].astype(str).str.strip().str.upper()
        df["TRANSACTION_ID"] = txn_clean.replace({
            "PENDING_TXN_ID": None,
            "UNKNOWN": None,
            "-": None,
            "": None,
            "NAN": None,
            "NONE": None
        })

    # 🟡 AMOUNT Cleaning ($ / Rs. stripping & negative -> NULL)
    if "AMOUNT" in df.columns:
        cleaned_amount_str = (
            df["AMOUNT"]
            .astype(str)
            .str.replace("$", "", regex=False)
            .str.replace("Rs.", "", regex=False)
            .str.strip()
        )
        cleaned_amount_num = pd.to_numeric(cleaned_amount_str, errors="coerce")
        df["AMOUNT"] = np.where(cleaned_amount_num < 0, None, cleaned_amount_num)

    # 🟡 PAYMENT_STATUS Normalization (e.g. success/SUCCESS -> SUCCESS, INVALID_STATUS -> NULL)
    if "PAYMENT_STATUS" in df.columns:
        status_clean = df["PAYMENT_STATUS"].astype(str).str.strip().str.upper()
        df["PAYMENT_STATUS"] = status_clean.replace({
            "INVALID_STATUS": None,
            "UNKNOWN_STATUS": None,
            "UNKNOWN": None,
            "-": None,
            "": None,
            "NAN": None,
            "NONE": None
        })

    # 🟡 PAYMENT_METHOD Normalization (e.g. PayPal/PAYPAL -> PAYPAL, UNKNOWN -> NULL)
    if "PAYMENT_METHOD" in df.columns:
        method_clean = df["PAYMENT_METHOD"].astype(str).str.strip().str.upper()
        df["PAYMENT_METHOD"] = method_clean.replace({
            "UNKNOWN": None,
            "-": None,
            "": None,
            "NAN": None,
            "NONE": None
        })

    # 🟡 TIMESTAMPS Normalization
    ts_cols = ["PAID_AT", "CREATED_AT", "UPDATED_AT"]
    for date_col in ts_cols:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # ---------------------------------------------------------
    # 4. SPLIT DATA: CLEAN vs QUARANTINE
    # ---------------------------------------------------------
    has_errors_mask = df["ERROR_REASONS"].apply(lambda errs: len(errs) > 0)

    # Quarantine records (Missing or Duplicate PAYMENT_ID)
    df_quarantine = df[has_errors_mask].copy()
    if not df_quarantine.empty:
        df_quarantine["ERROR_REASON"] = df_quarantine["ERROR_REASONS"].apply(lambda errs: "; ".join(errs))
        quarantine_cols = [c for c in ["PAYMENT_ID", "ORDER_ID", "TRANSACTION_ID", "ERROR_REASON"] if c in df_quarantine.columns]
        quarantine_to_write = df_quarantine[quarantine_cols].copy()
        
        session.write_pandas(
            quarantine_to_write,
            table_name="PAYMENT_MODEL",
            database="ECOMMERCE",
            schema="QUARANTINE",
            auto_create_table=False,
            overwrite=False
        )

    # Clean records
    df_clean = df[~has_errors_mask].copy()
    if df_clean.empty:
        return f"All records quarantined ({len(df_quarantine)} sent to QUARANTINE). No valid data to merge."

    # ---------------------------------------------------------
    # 5. ENFORCE DATA TYPES & TIMESTAMP FORMATS
    # ---------------------------------------------------------
    int_cols = ["PAYMENT_ID", "ORDER_ID"]
    for col in int_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce").astype("Int64")

    if "AMOUNT" in df_clean.columns:
        df_clean["AMOUNT"] = pd.to_numeric(df_clean["AMOUNT"], errors="coerce").astype("Float64")

    # Format Timestamps for Snowflake
    for date_col in ts_cols:
        if date_col in df_clean.columns:
            df_clean[date_col] = df_clean[date_col].dt.strftime("%Y-%m-%d %H:%M:%S")

    df_clean = df_clean.drop(columns=["ERROR_REASONS"])

    # ---------------------------------------------------------
    # 6. WRITE TO STAGING & EXECUTE MERGE
    # ---------------------------------------------------------
    session.sql("TRUNCATE TABLE ecommerce.temp.TEMP_CLEAN_PAYMENT_MODEL").collect()
    session.write_pandas(
        df_clean,
        table_name="TEMP_CLEAN_PAYMENT_MODEL",
        database="ECOMMERCE",
        schema="TEMP",
        auto_create_table=False,
        overwrite=True
    )

    merge_sql = """
    MERGE INTO ecommerce.CLEAN.CLEAN_PAYMENT_MODEL target
    USING ecommerce.TEMP.TEMP_CLEAN_PAYMENT_MODEL src
    ON target.PAYMENT_ID = src.PAYMENT_ID
    WHEN MATCHED THEN UPDATE SET
        target.ORDER_ID       = src.ORDER_ID,
        target.TRANSACTION_ID = src.TRANSACTION_ID,
        target.AMOUNT         = src.AMOUNT,
        target.PAYMENT_STATUS = src.PAYMENT_STATUS,
        target.PAYMENT_METHOD = src.PAYMENT_METHOD,
        target.PAID_AT        = src.PAID_AT,
        target.CREATED_AT     = src.CREATED_AT,
        target.UPDATED_AT     = src.UPDATED_AT
    WHEN NOT MATCHED THEN INSERT (
        PAYMENT_ID, ORDER_ID, TRANSACTION_ID, AMOUNT, PAYMENT_STATUS, PAYMENT_METHOD, PAID_AT, CREATED_AT, UPDATED_AT
    ) VALUES (
        src.PAYMENT_ID, src.ORDER_ID, src.TRANSACTION_ID, src.AMOUNT, src.PAYMENT_STATUS, src.PAYMENT_METHOD, src.PAID_AT, src.CREATED_AT, src.UPDATED_AT
    );
    """
    
    session.sql(merge_sql).collect()
    
    return f"Successfully processed {len(df)} records ({len(df_clean)} Merged into CLEAN, {len(df_quarantine)} sent to QUARANTINE)."
$$;
