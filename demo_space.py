import pandas as pd

from snowflake.snowpark.context import get_active_session

# Get Snowflake session
session = get_active_session()

print(session)

# Database
session.sql("USE DATABASE ECOMMERCE_DW").collect()

# Schema
session.sql("USE SCHEMA ECOMMERCE_DW.RAW_STAGES").collect()

# Read CSV from external stage
df = (
    session.read
    .format("csv")
    .option("SKIP_HEADER", 1)
    .load("@ECOMMERCE_DW.RAW_STAGES.EXTERNAL_STAGE_USER")
)




# Snowpark → Pandas
pdf = df.to_pandas()

# | Column              | Problem                      | Action in CLEAN                                    | Reason                                        |
# | ------------------- | ---------------------------- | -------------------------------------------------- | --------------------------------------------- |
# | `USER_ID`           | NULL / duplicate / invalid   | **DELETE ROW**                                     | Primary identifier                            |
# | `FIRST_NAME`        | NULL / empty                 | **DELETE ROW**                                     | Required user identity                        |
# | `LAST_NAME`         | NULL / empty                 | Usually **DELETE ROW**                             | Required user identity                        |
# | `MIDDLE_NAME`       | NULL                         | **KEEP**                                           | Middle name is optional                       |
# | `EMAIL`             | NULL / empty                 | **DELETE ROW**                                     | Required for user communication/login         |
# | `EMAIL`             | Invalid format               | **DELETE/QUARANTINE ROW**                          | Cannot reliably identify/contact user         |
# | `PASSWORD_HASH`     | NULL / empty                 | **DELETE/QUARANTINE ROW** if authentication user   | Required for authentication                   |
# | `PASSWORD_HASH`     | Plain password like `123456` | **DELETE/QUARANTINE**                              | Security/data-quality issue                   |
# | `STATUS`            | NULL                         | **DELETE or default only if business rule exists** | User state is important                       |
# | `ADDRESS_ID`        | NULL                         | **KEEP** if address optional                       | User can exist without address                |
# | `COUNTRY`           | NULL                         | **KEEP or quarantine**                             | Depends on business requirement               |
# | `STATE`             | NULL                         | **KEEP**                                           | Geographic details can be optional            |
# | `DISTRICT`          | NULL                         | **KEEP**                                           | Optional                                      |
# | `CITY`              | NULL                         | **KEEP**                                           | Depends on business requirement               |
# | `ZIPCODE`           | NULL                         | **KEEP**                                           | Don't delete user just because ZIP is missing |
# | `PHONE_NUMBER`      | NULL                         | **KEEP** if phone is optional                      | Email may be enough                           |
# | `PHONE_NUMBER`      | Invalid format               | **NULL it / quarantine**                           | Don't necessarily delete whole user           |
# | `ROLE_ID`           | NULL                         | **DELETE/QUARANTINE**                              | Authorization depends on role                 |
# | `ROLE_NAME`         | NULL                         | **DELETE/QUARANTINE**                              | Important authorization field                 |
# | `DEVICE_NAME`       | NULL                         | **KEEP**                                           | Device can be unknown                         |
# | `BROWSER`           | NULL                         | **KEEP**                                           | Browser may be unavailable                    |
# | `IP_ADDRESS`        | NULL                         | **KEEP**                                           | Not necessarily required                      |
# | `IP_ADDRESS`        | Invalid                      | **NULL it**                                        | Don't delete user because IP is bad           |
# | `LOGIN_STATUS`      | NULL                         | **KEEP**                                           | Login may not have happened                   |
# | `FULL_DATE`         | Invalid                      | **QUARANTINE**                                     | Important date field                          |
# | `DAY/MONTH/YEAR`    | Invalid                      | **REPAIR/RECALCULATE**                             | Derived fields                                |
# | `HOUR`              | 75                           | **REPAIR/NULL**                                    | Impossible value                              |
# | `MINUTE`            | 75                           | **REPAIR/NULL**                                    | Must be 0–59                                  |
# | `SECOND`            | 90                           | **REPAIR/NULL**                                    | Must be 0–59                                  |
# | `ACTIVITY_NAME`     | NULL                         | Depends                                            | Activity may be optional                      |
# | `ACTIVITY_COUNT`    | `five`                       | **CONVERT or NULL**                                | Expected numeric                              |
# | `NOTIFICATION_SENT` | negative                     | **NULL/quarantine**                                | Count cannot normally be negative             |
# | `NOTIFICATION_READ` | negative                     | **NULL/quarantine**                                | Count cannot normally be negative             |
# | `CREATED_AT`        | NULL/invalid                 | **DELETE/QUARANTINE**                              | Important audit field                         |
# | `UPDATED_AT`        | NULL/invalid                 | **KEEP/NULL** depending on model                   | Update may not exist                          |












# -----------------------------------------------------------------------------------------------------------------------------------------------------
session.sql("USE SCHEMA ECOMMERCE_DW.RAW_STAGES").collect()

# Read CSV from external stage
df = (
    session.read
    .format("csv")
    .option("SKIP_HEADER", 1)
    .load("@ECOMMERCE_DW.RAW_STAGES.EXTERNAL_STAGE_SELLER")
)

PDF = DF.TO_PANDAS()
# | Problem                     | Action             |
# | --------------------------- | ------------------ |
# | `SELLER_ID IS NULL`         | ❌ DELETE           |
# | `SELLER_PRODUCT_ID IS NULL` | ❌ DELETE           |
# | `PRODUCT_ID = INVALID_PROD` | ❌ DELETE           |
# | `QUANTITY < 0`              | ❌ DELETE           |
# | `ITEM_SELLING_PRICE < 0`    | ❌ DELETE           |
# | `DISCOUNT < 0`              | ❌ DELETE           |
# | `DISCOUNT > 100`            | ❌ DELETE           |
# | `QUANTITY = '50 units'`     | 🧹 CLEAN           |
# | `PRICE = '$442.4'`          | 🧹 CLEAN           |
# | `PRICE = 'Rs. 1866'`        | 🧹 CLEAN           |
# | `DISCOUNT = '19%'`          | 🧹 CLEAN           |
# | `USER_ID = 999999`          | 🧹 Convert to NULL |
# | `ROLE_ID = -1`              | 🧹 Convert to NULL |
# | GST NULL                    | 🧹 Keep            |
# | PAN NULL                    | 🧹 Keep            |
# | BUSINESS_NAME NULL          | 🧹 Keep/handle     |
# | EMAIL NULL                  | 🧹 Keep/handle     |


ID_NULL = PDF["C1"].isnull()
SP_ID_NULL = PDF["C2"].isnull()
PRODUCT_INVALID = PDF["C3"].eq("INVALID_PROD")
PROD_ID_NULL = PDF['C3'].ISNULL()

PDF["C4"] = (
    PDF["C4"]
    .astype(str)
    .str.extract(r"([-+]?\d*\.?\d+)", expand=False)
)

PDF["C4"] = pd.to_numeric(PDF["C4"], errors="coerce")

QUANTITY_NEGATIVE = PDF["C4"] < 0

PRICE_NEGATIVE = PDF["C5"] < 0
DISCOUNT_NEGATIVE = PDF["C6"] < 0
DISCOUNT_INVALID = PDF["C6"] > 

100



# PRICE: "$442.4" → 442.4
PDF["C5"] = (
    PDF["C5"]
    .astype(str)
    .str.extract(r"([-+]?\d*\.?\d+)", expand=False)
)

PDF["C5"] = pd.to_numeric(PDF["C5"], errors="coerce")


# PRICE: "Rs. 1866" → 1866
# Same logic as above


# DISCOUNT: "19%" → 19
PDF["C6"] = (
    PDF["C6"]
    .astype(str)
    .str.extract(r"([-+]?\d*\.?\d+)", expand=False)
)

PDF["C6"] = pd.to_numeric(PDF["C6"], errors="coerce")


PRICE_NEGATIVE = PDF["C5"] < 0
DISCOUNT_NEGATIVE = PDF["C6"] < 0
DISCOUNT_INVALID = PDF["C6"] > 100

PDF.loc[PDF["C7"] == 999999, "C7"] = pd.NA
PDF.loc[PDF["C8"] == -1, "C8"] = pd.NA


# -----------------------------------------------------------------------------------------------------------------------------------------------------

session.sql("USE SCHEMA ECOMMERCE_DW.RAW_STAGES").collect()

# Read CSV from external stage
df = (
    session.read
    .format("csv")
    .option("SKIP_HEADER", 1)
    .load("@ECOMMERCE_DW.RAW_STAGES.EXTERNAL_STAGE_Products")
)




# Snowpark → Pandas
pdf = df.to_pandas()



# -----------------------------------------------------------------------------------------------------------------------------------------------------



    


# | Problem                          | Action                                        |
# | -------------------------------- | --------------------------------------------- |
# | Duplicate `SHIPMENT_ID`          | 🔴 **DELETE/resolve duplicate record**        |
# | Missing `SHIPMENT_ID`            | 🔴 **DELETE if ID cannot be recovered**       |
# | Exact duplicate row              | 🔴 DELETE, but you have **0**                 |
# | `INVALID_STATUS`                 | 🟡 CLEAN → `NULL`                             |
# | `UNKNOWN_LOCATION`               | 🟡 CLEAN → `NULL`                             |
# | `UNKNOWN`                        | 🟡 CLEAN → `NULL`                             |
# | `-`                              | 🟡 CLEAN → `NULL`                             |
# | Negative shipping charge         | 🟡 Investigate → `NULL` if invalid            |
# | `$42.95`, `$35.2`                | 🟡 Remove `$`, convert to numeric             |
# | `FREE` shipping                  | 🟡 Convert to `0` if business meaning is free |
# | `2024-02-30`                     | 🟡 Invalid date → `NULL`                      |
# | Mixed date formats               | 🟡 Normalize                                  |
# | Missing `COURIER_NAME`           | 🟢 Keep                                       |
# | Missing `LOCATION`               | 🟢 Keep                                       |
# | Missing `STATUS`                 | 🟢 Keep                                       |
# | Missing `SHIPPED_AT`             | 🟢 Keep                                       |
# | Missing `DELIVERED_AT`           | 🟢 Keep                                       |
# | Missing `EXPECTED_DELIVERY_DATE` | 🟢 Keep                                       |


# =========================================================
# SHIPMENT_ID
# =========================================================

# Convert to numeric
PDF["SHIPMENT_ID"] = pd.to_numeric(
    PDF["SHIPMENT_ID"],
    errors="coerce"
)

# Missing SHIPMENT_ID
# If the ID cannot be recovered, remove the row
PDF = PDF.dropna(
    subset=["SHIPMENT_ID"]
)

# Remove duplicate SHIPMENT_ID
# Keep the first valid record
PDF = PDF.drop_duplicates(
    subset=["SHIPMENT_ID"],
    keep="first"
)


# =========================================================
# SHIPPING_CHARGE
# =========================================================

# Remove $ and Rs. and convert to numeric

PDF["SHIPPING_CHARGE"] = (
    PDF["SHIPPING_CHARGE"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace("Rs.", "", regex=False)
    .str.strip()
)

# FREE → 0
PDF["SHIPPING_CHARGE"] = PDF["SHIPPING_CHARGE"].replace({
    "FREE": "0"
})

# Convert to numeric
PDF["SHIPPING_CHARGE"] = pd.to_numeric(
    PDF["SHIPPING_CHARGE"],
    errors="coerce"
)

# Negative shipping charges are invalid
PDF.loc[
    PDF["SHIPPING_CHARGE"] < 0,
    "SHIPPING_CHARGE"
] = None


# =========================================================
# SHIPMENT_STATUS
# =========================================================

PDF["SHIPMENT_STATUS"] = (
    PDF["SHIPMENT_STATUS"]
    .astype(str)
    .str.strip()
    .str.upper()
)

# Invalid placeholder values → NULL
PDF["SHIPMENT_STATUS"] = PDF["SHIPMENT_STATUS"].replace({
    "INVALID_STATUS": None,
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# STATUS
# =========================================================

PDF["STATUS"] = (
    PDF["STATUS"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["STATUS"] = PDF["STATUS"].replace({
    "INVALID_STATUS": None,
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# LOCATION
# =========================================================

PDF["LOCATION"] = (
    PDF["LOCATION"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["LOCATION"] = PDF["LOCATION"].replace({
    "UNKNOWN_LOCATION": None,
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# COURIER_NAME
# =========================================================

PDF["COURIER_NAME"] = (
    PDF["COURIER_NAME"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["COURIER_NAME"] = PDF["COURIER_NAME"].replace({
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# TRACKING_NUMBER
# =========================================================

PDF["TRACKING_NUMBER"] = (
    PDF["TRACKING_NUMBER"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["TRACKING_NUMBER"] = PDF["TRACKING_NUMBER"].replace({
    "UNKNOWN": None,
    "TRACKING_PENDING": None,
    "-": None
})


# =========================================================
# SHIPPED_AT
# =========================================================

PDF["SHIPPED_AT"] = pd.to_datetime(
    PDF["SHIPPED_AT"],
    errors="coerce"
)


# =========================================================
# DELIVERED_AT
# =========================================================

PDF["DELIVERED_AT"] = pd.to_datetime(
    PDF["DELIVERED_AT"],
    errors="coerce"
)

# Delivery cannot happen before shipment
PDF.loc[
    PDF["DELIVERED_AT"] < PDF["SHIPPED_AT"],
    "DELIVERED_AT"
] = None


# =========================================================
# EXPECTED_DELIVERY_DATE
# =========================================================

PDF["EXPECTED_DELIVERY_DATE"] = pd.to_datetime(
    PDF["EXPECTED_DELIVERY_DATE"],
    errors="coerce",
    dayfirst=True
)


# =========================================================
# INVALID EXPECTED DELIVERY DATE
# =========================================================

# Expected delivery should not be before shipped date

PDF.loc[
    PDF["EXPECTED_DELIVERY_DATE"] <
    PDF["SHIPPED_AT"].dt.normalize(),
    "EXPECTED_DELIVERY_DATE"
] = None


# =========================================================
# UPDATED_AT
# =========================================================

PDF["UPDATED_AT"] = pd.to_datetime(
    PDF["UPDATED_AT"],
    errors="coerce"
)


# =========================================================
# ORDER_ID
# =========================================================

PDF["ORDER_ID"] = pd.to_numeric(
    PDF["ORDER_ID"],
    errors="coerce"
)


# =========================================================
# TRACKING_ID
# =========================================================

PDF["TRACKING_ID"] = pd.to_numeric(
    PDF["TRACKING_ID"],
    errors="coerce"
)


# -----------------------------------------------------------------------------------------------------------------------------------------------------


# | Data problem                      |                Count | Action                                  | Reason                                                      |
# | --------------------------------- | -------------------: | --------------------------------------- | ----------------------------------------------------------- |
# | Missing `RETURN_ID`               |           **2 rows** | 🔴 **DELETE if ID cannot be recovered** | `RETURN_ID` should uniquely identify a return               |
# | Exact duplicate row               |           **0 rows** | 🔴 DELETE if found                      | Completely duplicated records add no information            |
# | Duplicate/conflicting `RETURN_ID` | **27 rows involved** | 🔴 **RESOLVE, don't blindly delete**    | Same `RETURN_ID` is being used for different return records |


# | Data problem                                    |  Count | Action                                     | Reason                                              |
# | ----------------------------------------------- | -----: | ------------------------------------------ | --------------------------------------------------- |
# | `INVALID_STATUS`                                |  **8** | 🟡 → `NULL`                                | Invalid categorical value                           |
# | `-1`, `0`, `6`, `10` in `RATING`                | **14** | 🟡 → `NULL`                                | Rating should normally be 1–5                       |
# | Negative `REFUND_AMOUNT`                        |  **4** | 🟡 → `NULL` if invalid                     | Refund amount should normally not be negative       |
# | `$291.25` etc.                                  |      — | 🟡 Remove `$` → numeric                    | Formatting problem                                  |
# | `Processed`, `processed`, `PROCESSED`           |      — | 🟡 Normalize → `PROCESSED`                 | Same value with inconsistent casing                 |
# | `pending`, `PENDING`                            |      — | 🟡 Normalize → `PENDING`                   | Same status represented differently                 |
# | `Approved`, `approved`, `APPROVED`              |      — | 🟡 Normalize → `APPROVED`                  | Same status represented differently                 |
# | `In Transit`, `IN_TRANSIT`                      |      — | 🟡 Normalize → `IN_TRANSIT`                | Same status represented differently                 |
# | `defective`, `DEFECTIVE ITEM`, `Defective item` |      — | 🟡 Normalize                               | Same reason represented differently                 |
# | `Item_Not_Described`, `Item not as described`   |      — | 🟡 Normalize                               | Same reason represented differently                 |
# | `REFUNDED_AT < REQUESTED_AT`                    | **11** | 🟡 Fix timestamp → `NULL` if unrecoverable | Refund cannot normally happen before return request |
# | `REVIEWED_AT < REQUESTED_AT`                    |  **2** | 🟡 Fix timestamp → `NULL` if unrecoverable | Review should normally occur after request          |
# | Missing `REASON`                                | **33** | 🟢 Keep                                    | Missing reason doesn't invalidate the return        |
# | Missing `RETURN_STATUS`                         | **29** | 🟢 Keep                                    | Status can be unavailable                           |
# | Missing `REFUND_ID`                             | **78** | 🟢 Keep                                    | Not every return necessarily has a refund           |
# | Missing `REFUND_AMOUNT`                         | **36** | 🟢 Keep                                    | Refund may not yet exist                            |
# | Missing `REFUNDED_AT`                           | **85** | 🟢 Keep                                    | Normal for pending/rejected/non-refunded returns    |
# | Missing `REVIEW_ID`                             | **45** | 🟢 Keep                                    | Not every return has a review                       |
# | Missing `RATING`                                | **72** | 🟢 Keep                                    | Review/rating is optional                           |
# | Missing `COMMENT`                               | **82** | 🟢 Keep                                    | Comment is optional                                 |
# | Missing `REVIEWED_AT`                           | **65** | 🟢 Keep                                    | No review means no review timestamp                 |


# =========================================================
# RETURN_ID
# =========================================================

PDF["RETURN_ID"] = pd.to_numeric(
    PDF["RETURN_ID"],
    errors="coerce"
)

# Delete rows where RETURN_ID cannot be recovered

PDF = PDF.dropna(
    subset=["RETURN_ID"]
)


# =========================================================
# REASON
# =========================================================

PDF["REASON"] = (
    PDF["REASON"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["REASON"] = PDF["REASON"].replace({
    "DEFECTIVE ITEM": "DEFECTIVE",
    "DEFECTIVE": "DEFECTIVE",
    "ITEM NOT AS DESCRIBED": "ITEM_NOT_AS_DESCRIBED",
    "ITEM_NOT_DESCRIBED": "ITEM_NOT_AS_DESCRIBED",
    "WRONG ITEM": "WRONG_ITEM",
    "WRONG_ITEM": "WRONG_ITEM",
    "DAMAGED": "DAMAGED",
    "CHANGE OF MIND": "CHANGE_OF_MIND",
    "CHANGE_OF_MIND": "CHANGE_OF_MIND",
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# RETURN_STATUS
# =========================================================

PDF["RETURN_STATUS"] = (
    PDF["RETURN_STATUS"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["RETURN_STATUS"] = PDF["RETURN_STATUS"].replace({
    "INVALID_STATUS": None,
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# REQUESTED_AT
# =========================================================

PDF["REQUESTED_AT"] = pd.to_datetime(
    PDF["REQUESTED_AT"],
    errors="coerce"
)


# =========================================================
# REFUND_ID
# =========================================================

PDF["REFUND_ID"] = pd.to_numeric(
    PDF["REFUND_ID"],
    errors="coerce"
)


# =========================================================
# REFUND_AMOUNT
# =========================================================

PDF["REFUND_AMOUNT"] = (
    PDF["REFUND_AMOUNT"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace("Rs.", "", regex=False)
    .str.strip()
)

PDF["REFUND_AMOUNT"] = pd.to_numeric(
    PDF["REFUND_AMOUNT"],
    errors="coerce"
)

# Negative refund amount → NULL

PDF.loc[
    PDF["REFUND_AMOUNT"] < 0,
    "REFUND_AMOUNT"
] = None


# =========================================================
# REFUND_STATUS
# =========================================================

PDF["REFUND_STATUS"] = (
    PDF["REFUND_STATUS"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["REFUND_STATUS"] = PDF["REFUND_STATUS"].replace({
    "INVALID_STATUS": None,
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# REFUNDED_AT
# =========================================================

PDF["REFUNDED_AT"] = pd.to_datetime(
    PDF["REFUNDED_AT"],
    errors="coerce"
)

# Refund cannot happen before return request

PDF.loc[
    PDF["REFUNDED_AT"] < PDF["REQUESTED_AT"],
    "REFUNDED_AT"
] = None


# =========================================================
# REVIEW_ID
# =========================================================

PDF["REVIEW_ID"] = pd.to_numeric(
    PDF["REVIEW_ID"],
    errors="coerce"
)


# =========================================================
# RATING
# =========================================================

PDF["RATING"] = pd.to_numeric(PDF["RATING"],errors="coerce")

# Rating must be between 1 and 5

PDF.loc[~PDF["RATING"].between(1, 5),"RATING"] = None


# =========================================================
# COMMENT
# =========================================================

PDF["COMMENT"] = (PDF["COMMENT"].astype(str).str.strip())

PDF["COMMENT"] = PDF["COMMENT"].replace({"UNKNOWN": None,"-": None,"": None})


# =========================================================
# REVIEWED_AT
# =========================================================

PDF["REVIEWED_AT"] = pd.to_datetime(PDF["REVIEWED_AT"],errors="coerce")

# Review cannot happen before return request

PDF.loc[
    PDF["REVIEWED_AT"] < PDF["REQUESTED_AT"],"REVIEWED_AT"] = None




# ----------------------------------------------------------------------------------------------------------------------------------------------------

# | Data problem                    |      Count | Action                                  | Reason                                                    |
# | ------------------------------- | ---------: | --------------------------------------- | --------------------------------------------------------- |
# | Missing `ORDER_ID`              |  **1 row** | 🔴 **DELETE if ID cannot be recovered** | `ORDER_ID` is the main identifier for the order           |
# | Missing `ORDER_ITEM_ID`         | **6 rows** | 🔴 **DELETE if ID cannot be recovered** | An order-item record should have a unique item identifier |
# | Exact duplicate row             | **0 rows** | 🔴 DELETE if found                      | Completely duplicated record                              |
# | Duplicate `ORDER_ID`            |       Many | 🟢 **KEEP**                             | Multiple items can legitimately belong to the same order  |
# | `USER_ID = 999999`              |    Several | 🟡 CLEAN/NULL, **don't delete**         | Likely an unknown-user placeholder                        |
# | `QUANTITY = -2`                 |    Several | 🟡 CLEAN → `NULL`                       | Invalid quantity, but the order record may still be valid |
# | `UNIT_PRICE = -25`              |    Several | 🟡 CLEAN → `NULL`                       | Invalid price, but don't delete the entire order          |
# | `DISCOUNT_AMOUNT = -5`          |    Several | 🟡 CLEAN → `NULL`                       | Invalid discount value                                    |
# | `DISCOUNT_PERCENTAGE = -10%`    |    Several | 🟡 CLEAN → `NULL`                       | Invalid discount percentage                               |
# | `COUPON_CODE = INVALID_CODE`    |         11 | 🟡 CLEAN → `NULL`                       | Invalid coupon value                                      |
# | `ORDER_STATUS = UNKNOWN_STATUS` |    Several | 🟡 CLEAN → `NULL`                       | Invalid status                                            |
# | Missing `USER_ID`               |         38 | 🟢 KEEP                                 | User information can be unavailable                       |
# | Missing `TOTAL_AMOUNT`          |         22 | 🟢 KEEP                                 | Can potentially be recalculated                           |
# | Missing `SELLER_PRODUCT_ID`     |         84 | 🟢 KEEP                                 | Product reference may be unavailable                      |
# | Missing financial fields        |    Various | 🟢 KEEP                                 | Can potentially be recalculated or remain NULL            |


# =========================================================
# ORDER_ID
# =========================================================

PDF["ORDER_ID"] = pd.to_numeric(
    PDF["ORDER_ID"],
    errors="coerce"
)

# Delete rows where ORDER_ID cannot be recovered

PDF = PDF.dropna(
    subset=["ORDER_ID"]
)


# =========================================================
# ORDER_ITEM_ID
# =========================================================

PDF["ORDER_ITEM_ID"] = pd.to_numeric(
    PDF["ORDER_ITEM_ID"],
    errors="coerce"
)

# Delete rows where ORDER_ITEM_ID cannot be recovered

PDF = PDF.dropna(
    subset=["ORDER_ITEM_ID"]
)


# =========================================================
# DUPLICATE ORDER_ITEM_ID
# =========================================================

# Keep the first record if the complete item ID is duplicated

PDF = PDF.drop_duplicates(
    subset=["ORDER_ITEM_ID"],
    keep="first"
)


# =========================================================
# USER_ID
# =========================================================

PDF["USER_ID"] = pd.to_numeric(
    PDF["USER_ID"],
    errors="coerce"
)

# 999999 = unknown user

PDF["USER_ID"] = PDF["USER_ID"].replace({
    999999: None
})


# =========================================================
# QUANTITY
# =========================================================

PDF["QUANTITY"] = pd.to_numeric(
    PDF["QUANTITY"],
    errors="coerce"
)

# Quantity must be greater than 0

PDF.loc[
    PDF["QUANTITY"] <= 0,
    "QUANTITY"
] = None


# =========================================================
# UNIT_PRICE
# =========================================================

PDF["UNIT_PRICE"] = (
    PDF["UNIT_PRICE"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace("Rs.", "", regex=False)
    .str.strip()
)

PDF["UNIT_PRICE"] = pd.to_numeric(
    PDF["UNIT_PRICE"],
    errors="coerce"
)

# Negative price is invalid

PDF.loc[
    PDF["UNIT_PRICE"] < 0,
    "UNIT_PRICE"
] = None


# =========================================================
# DISCOUNT_AMOUNT
# =========================================================

PDF["DISCOUNT_AMOUNT"] = (
    PDF["DISCOUNT_AMOUNT"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace("Rs.", "", regex=False)
    .str.strip()
)

PDF["DISCOUNT_AMOUNT"] = pd.to_numeric(
    PDF["DISCOUNT_AMOUNT"],
    errors="coerce"
)

# Negative discount is invalid

PDF.loc[
    PDF["DISCOUNT_AMOUNT"] < 0,
    "DISCOUNT_AMOUNT"
] = None


# =========================================================
# DISCOUNT_PERCENTAGE
# =========================================================

PDF["DISCOUNT_PERCENTAGE"] = (
    PDF["DISCOUNT_PERCENTAGE"]
    .astype(str)
    .str.replace("%", "", regex=False)
    .str.strip()
)

PDF["DISCOUNT_PERCENTAGE"] = pd.to_numeric(
    PDF["DISCOUNT_PERCENTAGE"],
    errors="coerce"
)

# Discount percentage must be between 0 and 100

PDF.loc[
    ~PDF["DISCOUNT_PERCENTAGE"].between(0, 100),
    "DISCOUNT_PERCENTAGE"
] = None


# =========================================================
# COUPON_CODE
# =========================================================

PDF["COUPON_CODE"] = (
    PDF["COUPON_CODE"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["COUPON_CODE"] = PDF["COUPON_CODE"].replace({
    "INVALID_CODE": None,
    "UNKNOWN": None,
    "-": None,
    "": None
})


# =========================================================
# ORDER_STATUS
# =========================================================

PDF["ORDER_STATUS"] = (
    PDF["ORDER_STATUS"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["ORDER_STATUS"] = PDF["ORDER_STATUS"].replace({
    "UNKNOWN_STATUS": None,
    "INVALID_STATUS": None,
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# ORDER_DATE
# =========================================================

PDF["ORDER_DATE"] = pd.to_datetime(
    PDF["ORDER_DATE"],
    errors="coerce"
)


# =========================================================
# TOTAL_AMOUNT
# =========================================================

PDF["TOTAL_AMOUNT"] = (
    PDF["TOTAL_AMOUNT"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace("Rs.", "", regex=False)
    .str.strip()
)

PDF["TOTAL_AMOUNT"] = pd.to_numeric(
    PDF["TOTAL_AMOUNT"],
    errors="coerce"
)

# Negative total amount is invalid

PDF.loc[
    PDF["TOTAL_AMOUNT"] < 0,
    "TOTAL_AMOUNT"
] = None


# ----------------------------------------------------------------------------------------------------------------------------------------------------


# | Data problem                |                  Count | Action                                  | Reason                                                     |
# | --------------------------- | ---------------------: | --------------------------------------- | ---------------------------------------------------------- |
# | Missing `CART_ID`           |             **3 rows** | 🔴 **DELETE if ID cannot be recovered** | `CART_ID` identifies the cart                              |
# | Missing `CART_ITEM_ID`      |             **5 rows** | 🔴 **DELETE if ID cannot be recovered** | A cart-item record needs a unique item identifier          |
# | Exact duplicate row         |             **0 rows** | 🔴 DELETE if found                      | Completely duplicated record                               |
# | Duplicate `CART_ID`         |               **Many** | 🟢 **KEEP**                             | One cart can contain multiple cart items                   |
# | Duplicate `CART_ITEM_ID`    | **5 records involved** | 🔴 **RESOLVE**                          | Same item ID is assigned to different cart/product records |
# | Missing `USER_ID`           |            **10 rows** | 🟢 KEEP                                 | User information can be unavailable                        |
# | Missing `SELLER_PRODUCT_ID` |              **1 row** | 🟡 CLEAN/NULL                           | Product reference can potentially be recovered             |
# | `QUANTITY <= 0`             |            **12 rows** | 🟡 CLEAN → `NULL`                       | Invalid cart quantity                                      |
# | Negative `UNIT_PRICE`       |             **8 rows** | 🟡 CLEAN → `NULL`                       | Price cannot normally be negative                          |
# | `INVALID_STATUS`            |            **11 rows** | 🟡 CLEAN → `NULL`                       | Invalid status value                                       |
# | Mixed status capitalization |                   Many | 🟡 NORMALIZE                            | Same status represented differently                        |
# | Missing `CREATED_AT`        |            **14 rows** | 🟢 KEEP                                 | Timestamp can be unavailable                               |
# | Missing `UPDATED_AT`        |            **44 rows** | 🟢 KEEP                                 | Update timestamp can legitimately be missing               |
# | Missing `ADDED_AT`          |            **14 rows** | 🟢 KEEP                                 | Item-added timestamp can be unavailable                    |
# | Missing `WISHLIST_ID`       |           **103 rows** | 🟢 KEEP                                 | Not every cart item belongs to a wishlist                  |
# | Missing `WISHLIST_ADDED_AT` |           **120 rows** | 🟢 KEEP                                 | Normal when item isn't in wishlist                         |


# =========================================================
# CART_ID
# =========================================================

PDF["CART_ID"] = pd.to_numeric(
    PDF["CART_ID"],
    errors="coerce"
)

# Delete rows where CART_ID cannot be recovered

PDF = PDF.dropna(
    subset=["CART_ID"]
)


# =========================================================
# CART_ITEM_ID
# =========================================================

PDF["CART_ITEM_ID"] = pd.to_numeric(
    PDF["CART_ITEM_ID"],
    errors="coerce"
)

# Delete rows where CART_ITEM_ID cannot be recovered

PDF = PDF.dropna(
    subset=["CART_ITEM_ID"]
)


# =========================================================
# USER_ID
# =========================================================

PDF["USER_ID"] = pd.to_numeric(
    PDF["USER_ID"],
    errors="coerce"
)


# =========================================================
# SELLER_PRODUCT_ID
# =========================================================

PDF["SELLER_PRODUCT_ID"] = pd.to_numeric(
    PDF["SELLER_PRODUCT_ID"],
    errors="coerce"
)


# =========================================================
# QUANTITY
# =========================================================

PDF["QUANTITY"] = pd.to_numeric(
    PDF["QUANTITY"],
    errors="coerce"
)

# Quantity must be greater than 0

PDF.loc[
    PDF["QUANTITY"] <= 0,
    "QUANTITY"
] = None


# =========================================================
# UNIT_PRICE
# =========================================================

PDF["UNIT_PRICE"] = (
    PDF["UNIT_PRICE"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace("Rs.", "", regex=False)
    .str.strip()
)

PDF["UNIT_PRICE"] = pd.to_numeric(
    PDF["UNIT_PRICE"],
    errors="coerce"
)

# Negative price → NULL

PDF.loc[
    PDF["UNIT_PRICE"] < 0,
    "UNIT_PRICE"
] = None


# =========================================================
# STATUS
# =========================================================

PDF["STATUS"] = (
    PDF["STATUS"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["STATUS"] = PDF["STATUS"].replace({
    "INVALID_STATUS": None,
    "UNKNOWN_STATUS": None,
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# CREATED_AT
# =========================================================

PDF["CREATED_AT"] = pd.to_datetime(
    PDF["CREATED_AT"],
    errors="coerce"
)


# =========================================================
# UPDATED_AT
# =========================================================

PDF["UPDATED_AT"] = pd.to_datetime(
    PDF["UPDATED_AT"],
    errors="coerce"
)


# =========================================================
# ADDED_AT
# =========================================================

PDF["ADDED_AT"] = pd.to_datetime(
    PDF["ADDED_AT"],
    errors="coerce"
)


# =========================================================
# WISHLIST_ID
# =========================================================

PDF["WISHLIST_ID"] = pd.to_numeric(
    PDF["WISHLIST_ID"],
    errors="coerce"
)


# =========================================================
# WISHLIST_ADDED_AT
# =========================================================

PDF["WISHLIST_ADDED_AT"] = pd.to_datetime(
    PDF["WISHLIST_ADDED_AT"],
    errors="coerce"
)




# ----------------------------------------------------------------------------------------------------------------------------------------------------



# | Data problem               |                Count | Action                                  | Reason                                                               |
# | -------------------------- | -------------------: | --------------------------------------- | -------------------------------------------------------------------- |
# | Missing `PAYMENT_ID`       |           **3 rows** | 🔴 **DELETE if ID cannot be recovered** | `PAYMENT_ID` is the payment identifier                               |
# | Exact duplicate row        |           **0 rows** | 🔴 DELETE if found                      | Completely duplicated payment                                        |
# | Duplicate `PAYMENT_ID`     | **36 rows involved** | 🔴 **RESOLVE**                          | Same payment ID is assigned to different payment records             |
# | Missing `ORDER_ID`         |           **8 rows** | 🟡 CLEAN/NULL                           | Payment may still be identifiable through transaction ID             |
# | Missing `TRANSACTION_ID`   |           **8 rows** | 🟡 CLEAN/NULL                           | Transaction ID can be unavailable for some payment states            |
# | Missing `AMOUNT`           |           **8 rows** | 🟡 CLEAN/NULL                           | Amount may be recoverable or unavailable for failed/pending payments |
# | Negative `AMOUNT`          |           **9 rows** | 🟡 CLEAN → `NULL`                       | Payment amount should normally not be negative                       |
# | `INVALID_STATUS`           |           **9 rows** | 🟡 CLEAN → `NULL`                       | Invalid payment status                                               |
# | Mixed status casing        |                 Many | 🟡 Normalize                            | `success`, `SUCCESS`, `Success` are the same status                  |
# | Mixed payment method names |                 Many | 🟡 Normalize                            | `PayPal`, `PAYPAL`, `paypal` represent the same method               |
# | `PENDING_TXN_ID`           |                 Some | 🟡 CLEAN → `NULL`                       | Placeholder, not a real transaction ID                               |
# | Missing `PAID_AT`          |          **15 rows** | 🟢 KEEP                                 | Failed/pending payments may legitimately have no payment timestamp   |




# =========================================================
# PAYMENT_ID
# =========================================================

PDF["PAYMENT_ID"] = pd.to_numeric(
    PDF["PAYMENT_ID"],
    errors="coerce"
)

# Delete rows where PAYMENT_ID cannot be recovered

PDF = PDF.dropna(
    subset=["PAYMENT_ID"]
)


# =========================================================
# ORDER_ID
# =========================================================

PDF["ORDER_ID"] = pd.to_numeric(
    PDF["ORDER_ID"],
    errors="coerce"
)


# =========================================================
# TRANSACTION_ID
# =========================================================

PDF["TRANSACTION_ID"] = (
    PDF["TRANSACTION_ID"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["TRANSACTION_ID"] = PDF["TRANSACTION_ID"].replace({
    "PENDING_TXN_ID": None,
    "UNKNOWN": None,
    "-": None,
    "": None
})


# =========================================================
# AMOUNT
# =========================================================

PDF["AMOUNT"] = (
    PDF["AMOUNT"]
    .astype(str)
    .str.replace("$", "", regex=False)
    .str.replace("Rs.", "", regex=False)
    .str.strip()
)

PDF["AMOUNT"] = pd.to_numeric(
    PDF["AMOUNT"],
    errors="coerce"
)

# Negative payment amount → NULL

PDF.loc[
    PDF["AMOUNT"] < 0,
    "AMOUNT"
] = None


# =========================================================
# PAYMENT_STATUS
# =========================================================

PDF["PAYMENT_STATUS"] = (
    PDF["PAYMENT_STATUS"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["PAYMENT_STATUS"] = PDF["PAYMENT_STATUS"].replace({
    "INVALID_STATUS": None,
    "UNKNOWN_STATUS": None,
    "UNKNOWN": None,
    "-": None
})


# =========================================================
# PAYMENT_METHOD
# =========================================================

PDF["PAYMENT_METHOD"] = (
    PDF["PAYMENT_METHOD"]
    .astype(str)
    .str.strip()
    .str.upper()
)

PDF["PAYMENT_METHOD"] = PDF["PAYMENT_METHOD"].replace({
    "UNKNOWN": None,
    "-": None,
    "": None
})


# =========================================================
# PAID_AT
# =========================================================

PDF["PAID_AT"] = pd.to_datetime(
    PDF["PAID_AT"],
    errors="coerce"
)


# =========================================================
# CREATED_AT
# =========================================================

PDF["CREATED_AT"] = pd.to_datetime(
    PDF["CREATED_AT"],
    errors="coerce"
)


# =========================================================
# UPDATED_AT
# =========================================================

PDF["UPDATED_AT"] = pd.to_datetime(
    PDF["UPDATED_AT"],
    errors="coerce"
)

# =======================================================================================================================================================================================# | Problem                                       | Action        | Reason                                         |
# | --------------------------------------------- | ------------- | ---------------------------------------------- |
# | `PRODUCT_ID = NULL`                           | 🔴 **DELETE** | Cannot reliably identify parent product        |
# | `VARIANT_ID = NULL`                           | 🔴 **DELETE** | Variant cannot be uniquely identified          |
# | Invalid primary/business key with no recovery | 🔴 **DELETE** | Record cannot be reliably reconstructed        |
# | `BRAND_ID = 999`                              | 🟡 **FIX**    | Invalid/sentinel ID                            |
# | Duplicate SKU                                 | 🟡 **FIX**    | Attribute/key problem, not necessarily bad row |
# | NULL `PRODUCT_NAME`                           | 🟡 **FIX**    | Recover from product master if possible        |
# | NULL `MODEL`                                  | 🟢 KEEP       | Missing optional attribute                     |
# | NULL `COLOR`                                  | 🟢 KEEP       | May be legitimately unknown/not applicable     |
# | NULL `SIZE`                                   | 🟢 KEEP       | May be legitimately unknown/not applicable     |
# | NULL `STORAGE`                                | 🟢 KEEP       | Not applicable to every product                |
# | NULL `IMAGE_ID`                               | 🟢 KEEP       | Image is optional                              |
# | NULL `DESCRIPTION`                            | 🟢 KEEP/FIX   | Descriptive field                              |
# | `$`, `Rs.` in price                           | 🟡 **FIX**    | Formatting issue                               |
# | `kg`, `g` in weight                           | 🟡 **FIX**    | Unit normalization                             |
# | `cm`, `mm`, `inches` dimensions               | 🟡 **FIX**    | Unit normalization                             |
# | `APPLE` / `Apple` / `apple`                   | 🟡 **FIX**    | Standardization                                |
# | `Shoes` / `Footwear`                          | 🟡 **FIX**    | Category standardization                       |
