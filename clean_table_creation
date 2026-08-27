// ===============================================================================================================================================
use ecommerce;
create schema if not exists clean;
use schema clean;
// ===============================================================================================================================================

CREATE TABLE if not exists clean.Clean_USER_MODEL (

    USER_ID NUMBER,
    FIRST_NAME VARCHAR(100),
    LAST_NAME VARCHAR(100),
    MIDDLE_NAME VARCHAR(100),
    EMAIL VARCHAR(255),
    PASSWORD_HASH VARCHAR(500),
    STATUS BOOLEAN,

    ADDRESS_ID NUMBER,
    COUNTRY VARCHAR(100),
    STATE VARCHAR(100),
    DISTRICT VARCHAR(100),
    CITY VARCHAR(100),
    VILLAGE VARCHAR(100),
    ZIPCODE VARCHAR(20),
    ADDRESS_TYPE VARCHAR(50),

    MOBILE_ID NUMBER,
    COUNTRY_CODE VARCHAR(10),
    PHONE_NUMBER VARCHAR(20),
    IS_PRIMARY BOOLEAN,

    ROLE_ID NUMBER,
    ROLE_NAME VARCHAR(100),

    DEVICE_NAME VARCHAR(100),
    BROWSER VARCHAR(100),
    IP_ADDRESS VARCHAR(45),
    LOGIN_STATUS VARCHAR(50),

    FULL_DATE DATE,
    DAY NUMBER,
    MONTH NUMBER,
    QUARTER NUMBER,
    YEAR NUMBER,
    DAY_NAME VARCHAR(20),

    HOUR NUMBER,
    MINUTE NUMBER,
    SECOND NUMBER,

    ACTIVITY_NAME VARCHAR(100),
    ACTIVITY_COUNT NUMBER,

    NOTIFICATION_TYPE VARCHAR(100),
    NOTIFICATION_SENT NUMBER,
    NOTIFICATION_READ NUMBER,

    CREATED_AT TIMESTAMP_NTZ,
    UPDATED_AT TIMESTAMP_NTZ
);



CREATE  TABLE if not exists clean.clean_SELLER_MODEL (

    SELLER_ID NUMBER,

    GST_NUMBER VARCHAR(20),

    PAN VARCHAR(10),

    BUSINESS_NAME VARCHAR(255),

    BUSINESS_EMAIL VARCHAR(255),

    USER_ID NUMBER,

    ROLE_ID NUMBER,

    SELLER_PRODUCT_ID NUMBER,

    PRODUCT_ID NUMBER,

    ITEM_SELLING_PRICE NUMBER(10,2),

    QUANTITY NUMBER,

    DISCOUNT_PERCENTAGE NUMBER(5,2)

);

CREATE  TABLE if not exists clean.clean_ORDER_MODEL (

    ORDER_ID NUMBER,

    USER_ID NUMBER,

    TOTAL_AMOUNT NUMBER(10,2),

    ORDER_STATUS VARCHAR(30),

    ORDER_ITEM_ID NUMBER,

    SELLER_PRODUCT_ID NUMBER,

    QUANTITY NUMBER,

    UNIT_PRICE NUMBER(10,2),

    COST_PRICE NUMBER(10,2),

    DISCOUNT_AMOUNT NUMBER(10,2),

    TAX_AMOUNT NUMBER(10,2),

    SHIPPING_AMOUNT NUMBER(10,2),

    REVENUE NUMBER(12,2),

    PROFIT NUMBER(12,2),

    MARGIN_PERCENT NUMBER(5,2),

    COUPON_ID NUMBER,

    COUPON_CODE VARCHAR(30),

    DISCOUNT_PERCENTAGE NUMBER(5,2),

    MINIMUM_AMOUNT NUMBER(10,2),

    EXPIRY_DATE DATE,

    IS_ACTIVE BOOLEAN

);
CREATE TABLE if not exists clean.clean_RETURN_REFUND_MODEL (

    RETURN_ID NUMBER,

    ORDER_ITEM_ID NUMBER,

    REASON VARCHAR(200),

    RETURN_STATUS VARCHAR(30),

    REQUESTED_AT TIMESTAMP_NTZ,

    REFUND_ID NUMBER,

    REFUND_AMOUNT NUMBER(10,2),

    REFUND_STATUS VARCHAR(30),

    REFUNDED_AT TIMESTAMP_NTZ,

    REVIEW_ID NUMBER,

    RATING NUMBER(1),

    COMMENT VARCHAR(500),

    REVIEWED_AT TIMESTAMP_NTZ

);
CREATE TABLE if not exists clean.clean_SHIPMENTS_MODEL (

    SHIPMENT_ID NUMBER,

    ORDER_ID NUMBER,

    TRACKING_ID NUMBER,

    TRACKING_NUMBER VARCHAR(100),

    COURIER_NAME VARCHAR(100),

    SHIPMENT_STATUS VARCHAR(30),

    SHIPPING_CHARGE NUMBER(10,2),

    SHIPPED_AT TIMESTAMP_NTZ,

    EXPECTED_DELIVERY_DATE DATE,

    DELIVERED_AT TIMESTAMP_NTZ,

    LOCATION VARCHAR(100),

    STATUS VARCHAR(50),

    UPDATED_AT TIMESTAMP_NTZ

);


CREATE TABLE if not exists clean.clean_CART_MODEL (

    CART_ID NUMBER,

    USER_ID NUMBER,

    CREATED_AT TIMESTAMP_NTZ,
    UPDATED_AT TIMESTAMP_NTZ,

    STATUS VARCHAR(20),

    CART_ITEM_ID NUMBER,

    SELLER_PRODUCT_ID NUMBER,

    QUANTITY NUMBER,

    UNIT_PRICE NUMBER(10,2),

    ADDED_AT TIMESTAMP_NTZ,

    WISHLIST_ID NUMBER,

    WISHLIST_ADDED_AT TIMESTAMP_NTZ

);
CREATE TABLE if not exists clean.clean_PAYMENTS_MODEL (

    PAYMENT_ID NUMBER,

    ORDER_ID NUMBER,

    PAYMENT_METHOD_ID NUMBER,
    METHOD_NAME VARCHAR(30),

    AMOUNT NUMBER(10,2),

    PAYMENT_STATUS VARCHAR(30),

    TRANSACTION_ID VARCHAR(100),

    PAID_AT TIMESTAMP_NTZ

);

CREATE TABLE if not exists clean.PRODUCT_MODEL (

    BRAND_ID NUMBER,
    BRAND_NAME VARCHAR(100),

    CATEGORY_ID NUMBER,
    CATEGORY_NAME VARCHAR(100),

    PRODUCT_ID NUMBER,
    PRODUCT_NAME VARCHAR(100),

    MODEL VARCHAR(100),
    PRODUCT_MARKET_PRICE NUMBER(10,2),

    DESCRIPTION VARCHAR(500),

    WEIGHT NUMBER(10,2),
    DIMENSIONS VARCHAR(200),

    CREATED_AT TIMESTAMP_NTZ,

    IMAGE_ID NUMBER,
    IMAGE_URL VARCHAR(500),
    IS_PRIMARY BOOLEAN,
    UPLOADED_AT TIMESTAMP_NTZ,

    VARIANT_ID NUMBER,
    COLOR VARCHAR(30),
    SIZE VARCHAR(20),
    STORAGE VARCHAR(20),

    SKU VARCHAR(100)

);
// ===============================================================================================================================================
-- clean_streams
// ===============================================================================================================================================
create  stream if not exists clean.clean_user_stream
on table clean.clean_user_Model;

create  stream if not exists clean.CLEAN_CART_stream
on table clean.CLEAN_CART_MODEL;

create  stream if not exists clean.CLEAN_ORDER_stream
on table clean.CLEAN_ORDER_MODEL;

create  stream if not exists clean.CLEAN_PAYMENTS_stream
on table clean.CLEAN_PAYMENTS_MODEL;

create  stream if not exists clean.CLEAN_PRODUCT_stream
on table clean.CLEAN_PRODUCT_MODEL;

create  stream if not exists clean.CLEAN_RETURN_REFUND_stream
on table clean.CLEAN_RETURN_REFUND_MODEL;

create  stream if not exists clean.CLEAN_SELLER_stream
on table clean.CLEAN_SELLER_MODEL;

create  stream if not exists clean.CLEAN_SHIPMENTS_stream
on table clean.CLEAN_SHIPMENTS_MODEL;
// ===============================================================================================================================================
-- file format
// ===============================================================================================================================================
create or replace file format  PAYMENTS_MODEL_file
type = csv
skip_header = 1;

create or replace file format  CART_MODEL_file
type = csv
skip_header = 1 ;

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
type = csv
skip_header = 1;
// ===============================================================================================================================================
-- CLEAN_STAGES
// ===============================================================================================================================================

USE SCHEMA CLEAN_STAGES;

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
