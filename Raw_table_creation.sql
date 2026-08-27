// =============================================================================================================================================== 
-- Raw_tables
// ===============================================================================================================================================


-- 1. RAW.USER_MODEL
CREATE OR REPLACE TABLE RAW.USER_MODEL (
    USER_ID VARCHAR(255),
    FIRST_NAME VARCHAR(255),
    LAST_NAME VARCHAR(255),
    MIDDLE_NAME VARCHAR(255),
    EMAIL VARCHAR(255),
    PASSWORD_HASH VARCHAR(500),
    STATUS VARCHAR(50),
    ADDRESS_ID VARCHAR(255),
    COUNTRY VARCHAR(100),
    STATE VARCHAR(100),
    DISTRICT VARCHAR(100),
    CITY VARCHAR(100),
    VILLAGE VARCHAR(100),
    ZIPCODE VARCHAR(50),
    ADDRESS_TYPE VARCHAR(50),
    MOBILE_ID VARCHAR(255),
    COUNTRY_CODE VARCHAR(50),
    PHONE_NUMBER VARCHAR(50),
    IS_PRIMARY VARCHAR(50),
    ROLE_ID VARCHAR(255),
    ROLE_NAME VARCHAR(100),
    DEVICE_NAME VARCHAR(100),
    BROWSER VARCHAR(100),
    IP_ADDRESS VARCHAR(100),
    LOGIN_STATUS VARCHAR(50),
    FULL_DATE VARCHAR(50),
    DAY VARCHAR(50),
    MONTH VARCHAR(50),
    QUARTER VARCHAR(50),
    YEAR VARCHAR(50),
    DAY_NAME VARCHAR(50),
    HOUR VARCHAR(50),
    MINUTE VARCHAR(50),
    SECOND VARCHAR(50),
    ACTIVITY_NAME VARCHAR(100),
    ACTIVITY_COUNT VARCHAR(50),
    NOTIFICATION_TYPE VARCHAR(100),
    NOTIFICATION_SENT VARCHAR(50),
    NOTIFICATION_READ VARCHAR(50),
    CREATED_AT VARCHAR(100),
    UPDATED_AT VARCHAR(100)
);

-- 2. RAW.SELLER_MODEL
CREATE OR REPLACE TABLE RAW.SELLER_MODEL (
    SELLER_ID VARCHAR(255),
    GST_NUMBER VARCHAR(50),
    PAN VARCHAR(50),
    BUSINESS_NAME VARCHAR(255),
    BUSINESS_EMAIL VARCHAR(255),
    USER_ID VARCHAR(255),
    ROLE_ID VARCHAR(255),
    SELLER_PRODUCT_ID VARCHAR(255),
    PRODUCT_ID VARCHAR(255),
    ITEM_SELLING_PRICE VARCHAR(100),
    QUANTITY VARCHAR(100),
    DISCOUNT_PERCENTAGE VARCHAR(100)
);

-- 3. RAW.ORDER_MODEL
CREATE OR REPLACE TABLE RAW.ORDER_MODEL (
    ORDER_ID VARCHAR(255),
    USER_ID VARCHAR(255),
    TOTAL_AMOUNT VARCHAR(100),
    ORDER_STATUS VARCHAR(100),
    ORDER_ITEM_ID VARCHAR(255),
    SELLER_PRODUCT_ID VARCHAR(255),
    QUANTITY VARCHAR(100),
    UNIT_PRICE VARCHAR(100),
    COST_PRICE VARCHAR(100),
    DISCOUNT_AMOUNT VARCHAR(100),
    TAX_AMOUNT VARCHAR(100),
    SHIPPING_AMOUNT VARCHAR(100),
    REVENUE VARCHAR(100),
    PROFIT VARCHAR(100),
    MARGIN_PERCENT VARCHAR(100),
    COUPON_ID VARCHAR(255),
    COUPON_CODE VARCHAR(100),
    DISCOUNT_PERCENTAGE VARCHAR(100),
    MINIMUM_AMOUNT VARCHAR(100),
    EXPIRY_DATE VARCHAR(100),
    IS_ACTIVE VARCHAR(50)
);

-- 4. RAW.RETURN_REFUND_MODEL
CREATE OR REPLACE TABLE RAW.RETURN_REFUND_MODEL (
    RETURN_ID VARCHAR(255),
    ORDER_ITEM_ID VARCHAR(255),
    REASON VARCHAR(255),
    RETURN_STATUS VARCHAR(100),
    REQUESTED_AT VARCHAR(100),
    REFUND_ID VARCHAR(255),
    REFUND_AMOUNT VARCHAR(100),
    REFUND_STATUS VARCHAR(100),
    REFUNDED_AT VARCHAR(100),
    REVIEW_ID VARCHAR(255),
    RATING VARCHAR(50),
    COMMENT VARCHAR(1000),
    REVIEWED_AT VARCHAR(100)
);

-- 5. RAW.SHIPMENTS_MODEL
CREATE OR REPLACE TABLE RAW.SHIPMENTS_MODEL (
    SHIPMENT_ID VARCHAR(255),
    ORDER_ID VARCHAR(255),
    TRACKING_ID VARCHAR(255),
    TRACKING_NUMBER VARCHAR(100),
    COURIER_NAME VARCHAR(100),
    SHIPMENT_STATUS VARCHAR(100),
    SHIPPING_CHARGE VARCHAR(100),
    SHIPPED_AT VARCHAR(100),
    EXPECTED_DELIVERY_DATE VARCHAR(100),
    DELIVERED_AT VARCHAR(100),
    LOCATION VARCHAR(255),
    STATUS VARCHAR(100),
    UPDATED_AT VARCHAR(100)
);

-- 6. RAW.CART_MODEL
CREATE OR REPLACE TABLE RAW.CART_MODEL (
    CART_ID VARCHAR(255),
    USER_ID VARCHAR(255),
    CREATED_AT VARCHAR(100),
    UPDATED_AT VARCHAR(100),
    STATUS VARCHAR(100),
    CART_ITEM_ID VARCHAR(255),
    SELLER_PRODUCT_ID VARCHAR(255),
    QUANTITY VARCHAR(100),
    UNIT_PRICE VARCHAR(100),
    ADDED_AT VARCHAR(100),
    WISHLIST_ID VARCHAR(255),
    WISHLIST_ADDED_AT VARCHAR(100)
);

-- 7. RAW.PAYMENTS_MODEL
CREATE OR REPLACE TABLE RAW.PAYMENTS_MODEL (
    PAYMENT_ID VARCHAR(255),
    ORDER_ID VARCHAR(255),
    PAYMENT_METHOD_ID VARCHAR(255),
    METHOD_NAME VARCHAR(100),
    AMOUNT VARCHAR(100),
    PAYMENT_STATUS VARCHAR(100),
    TRANSACTION_ID VARCHAR(255),
    PAID_AT VARCHAR(100)
);

-- 8. RAW.PRODUCT_MODEL
CREATE OR REPLACE TABLE RAW.PRODUCT_MODEL (
    BRAND_ID VARCHAR(255),
    BRAND_NAME VARCHAR(100),
    CATEGORY_ID VARCHAR(255),
    CATEGORY_NAME VARCHAR(100),
    PRODUCT_ID VARCHAR(255),
    PRODUCT_NAME VARCHAR(255),
    MODEL VARCHAR(100),
    PRODUCT_MARKET_PRICE VARCHAR(100),
    DESCRIPTION VARCHAR(1000),
    WEIGHT VARCHAR(100),
    DIMENSIONS VARCHAR(255),
    CREATED_AT VARCHAR(100),
    IMAGE_ID VARCHAR(255),
    IMAGE_URL VARCHAR(500),
    IS_PRIMARY VARCHAR(50),
    UPLOADED_AT VARCHAR(100),
    VARIANT_ID VARCHAR(255),
    COLOR VARCHAR(50),
    SIZE VARCHAR(50),
    STORAGE VARCHAR(50),
    SKU VARCHAR(100)
);

// ===============================================================================================================================================
-- Raw_Stream
// ===============================================================================================================================================
create or replace stream Raw_stream_shipments
on table RAW.shipments_MODEL;

create or replace stream Raw_stream_seller
on table RAW.seller_MODEL;

create or replace stream Raw_stream_RETURN_REFUND
on table RAW.Return_refund_MODEL;

create or replace stream Raw_stream_product
on table RAW.product_MODEL;

create or replace stream Raw_stream_PAYMENTS
on table RAW.PAYMENTS_MODEL;

create or replace stream Raw_stream_user
on table RAW.USER_MODEL;

create or replace stream Raw_stream_cart
on table RAW.cart_MODEL;

create or replace stream Raw_stream_ORDER
on table RAW.ORDER_MODEL;

// ===============================================================================================================================================
-- Raw_File_format
// ===============================================================================================================================================
use schema raw;
create or replace file format  PAYMENTS_MODEL_file
type = csv
skip_header = 1;

create or replace file format  CART_MODEL_file
type = csv
skip_header = 1; 

create or replace file format  ORDER_MODEL_file
type = csv
skip_header = 1 ;

create or replace file format  PRODUCT_MODEL_file
type = csv
skip_header = 1;

create or replace file format  RETURN_REFUND_file
type = csv
skip_header = 1;

create or replace file format  SELLER_MODEL_file
type = csv
skip_header = 1;

create or replace file format  SHIPMENTS_MODEL_file
type = csv
skip_header = 1;

create or replace file format  USER_MODEL_file
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'         -- Handles fields containing commas (e.g. "Main St, City")
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE     -- Ignores extra commas/columns on a row instead of failing
    EMPTY_FIELD_AS_NULL = TRUE                 -- Converts empty strings to NULL
    NULL_IF = ('', 'NULL', 'null', 'N/A', 'n/a')-- Converts 'N/A' strings to NULL
    TRIM_SPACE = TRUE                          -- Removes leading/trailing spaces
    REPLACE_INVALID_CHARACTERS = TRUE;
// ===============================================================================================================================================
-- Raw_stages
// ===============================================================================================================================================

CREATE OR REPLACE STAGE PAYMENTS_STAGE
FILE_FORMAT = PAYMENTS_MODEL_file;

CREATE OR REPLACE STAGE CART_STAGE
FILE_FORMAT = CART_MODEL_file;

CREATE OR REPLACE STAGE ORDER_STAGE
FILE_FORMAT = ORDER_MODEL_file;

CREATE OR REPLACE STAGE PRODUCT_STAGE
FILE_FORMAT = PRODUCT_MODEL_file;

CREATE OR REPLACE STAGE RETURN_REFUND_STAGE
FILE_FORMAT = RETURN_REFUND_file;

CREATE OR REPLACE STAGE SELLER_STAGE
FILE_FORMAT = SELLER_MODEL_file;

CREATE OR REPLACE STAGE SHIPMENTS_STAGE
FILE_FORMAT = SHIPMENTS_MODEL_file;

CREATE OR REPLACE STAGE USER_STAGE
FILE_FORMAT = USER_MODEL_file;



-- Use schema ;

-- SHOW STAGES IN SCHEMA ecommerce_dw.RAW_STAGES;

-- create or Replace schema Raw;

-- CREATE OR REPLACE TABLE RAW.User_Model
-- USING TEMPLATE (
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
--     FROM TABLE(
--         INFER_SCHEMA(
--             LOCATION => '@RAW_STAGES.User_STAGE',
--             FILE_FORMAT => 'RAW_STAGES.User_MODEL_FILE'
--         )
--     )
-- );

-- CREATE OR REPLACE TABLE RAW.Seller_MODEL
-- USING TEMPLATE (
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
--     FROM TABLE(
--         INFER_SCHEMA(
--             LOCATION => '@RAW_STAGES.Seller_STAGE',
--             FILE_FORMAT => 'RAW_STAGES.Seller_MODEL_FILE'
--         )
--     )
-- );
-- CREATE OR REPLACE TABLE RAW.Shipments_MODEL
-- USING TEMPLATE (
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
--     FROM TABLE(
--         INFER_SCHEMA(
--             LOCATION => '@RAW_STAGES.Shipments_STAGE',
--             FILE_FORMAT => 'RAW_STAGES.Shipments_MODEL_FILE'
--         )
--     )
-- );
-- CREATE OR REPLACE TABLE RAW.Return_refund_MODEL
-- USING TEMPLATE (
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
--     FROM TABLE(
--         INFER_SCHEMA(
--             LOCATION => '@RAW_STAGES.Return_refund_STAGE',
--             FILE_FORMAT => 'RAW_STAGES.Return_refund_MODEL_FILE'
--         )
--     )
-- );
-- CREATE OR REPLACE TABLE RAW.Product_MODEL
-- USING TEMPLATE (
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
--     FROM TABLE(
--         INFER_SCHEMA(
--             LOCATION => '@RAW_STAGES.Product_STAGE',
--             FILE_FORMAT => 'RAW_STAGES.Product_MODEL_FILE'
--         )
--     )
-- );
-- CREATE OR REPLACE TABLE RAW.Payments_MODEL
-- USING TEMPLATE (
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
--     FROM TABLE(
--         INFER_SCHEMA(
--             LOCATION => '@RAW_STAGES.Payments_STAGE',
--             FILE_FORMAT => 'RAW_STAGES.Payments_MODEL_FILE'
--         )
--     )
-- );

-- CREATE OR REPLACE TABLE RAW.Order_MODEL
-- USING TEMPLATE (
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
--     FROM TABLE(
--         INFER_SCHEMA(
--             LOCATION => '@RAW_STAGES.ORDER_STAGE',
--             FILE_FORMAT => 'RAW_STAGES.ORDER_MODEL_FILE'
--         )
--     )
-- );

-- CREATE OR REPLACE TABLE RAW.Cart_MODEL
-- USING TEMPLATE (
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
--     FROM TABLE(
--         INFER_SCHEMA(
--             LOCATION => '@RAW_STAGES.Cart_STAGE',
--             FILE_FORMAT => 'RAW_STAGES.Cart_MODEL_FILE'
--         )
--     )
-- );


// =============================================================================================================================================== 

-- create or replace external table Shipments_Raw_Table
-- location = @external_stage_SHIPMENTS
-- file format = (type = csv)

-- create or replace external table User_Raw_Table
-- location = @external_stage_USER
-- file format = (type = csv)

-- create or replace external table SELLER_Raw_Table
-- location = @external_stage_SELLER
-- file format = (type = csv)


-- create or replace external table Return_refund_Raw_Table
-- location = @external_stage_RETURN_REFUND
-- file format = (type = csv)

-- create or replace external table Product_Raw_Table
-- location = @external_stage_Product
-- file format = (type = csv)

-- create or replace external table PAYMENTS_Raw_Table
-- location = @external_stage_PAYMENTS
-- file format = (type = csv)


-- create or replace external table Order_Raw_Table
-- location = @external_stage_ORDER
-- file format = (type = csv)


-- create or replace external table Cart_Raw_Table
-- location = @external_stage_Cart
-- file format = (type = csv)
