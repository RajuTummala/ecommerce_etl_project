// ===============================================================================================================================================
-- Connection between S3 Bucket to Snowflake
// ===============================================================================================================================================

CREATE or replace STORAGE INTEGRATION S3_external_table
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::625693793411:role/S3_To_snowFlake'
STORAGE_ALLOWED_LOCATIONS = ('s3://ecommerce-datasets-bucket')
STORAGE_AWS_EXTERNAL_ID = '12344321';

create or replace stage external_stage_Cart
url = 's3://ecommerce-datasets-bucket/CART_Model/'
storage_integration = S3_external_table
file_format = Cart_MODEL_FILE;


create or replace stage external_stage_ORDER
url = 's3://ecommerce-datasets-bucket/ORDER_Model/'
storage_integration = S3_external_table
file_format = ORDER_MODEL_FILE;


create or replace stage external_stage_PAYMENTS
url = 's3://ecommerce-datasets-bucket/PAYMENTS_Model/'
storage_integration = S3_external_table
file_format = PAYMENTS_MODEL_FILE;

create or replace stage external_stage_PRODUCT
url = 's3://ecommerce-datasets-bucket/PRODUCT_Model/'
storage_integration = S3_external_table
file_format = Product_MODEL_FILE;



create or replace stage external_stage_RETURN_REFUND
url = 's3://ecommerce-datasets-bucket/RETURN_REFUND_Model/'
storage_integration = S3_external_table
file_format = Return_refund_FILE;


create or replace stage external_stage_SELLER
url = 's3://ecommerce-datasets-bucket/SELLER_Model/'
storage_integration = S3_external_table
file_format = Seller_MODEL_FILE;


create or replace stage external_stage_USER
url = 's3://ecommerce-datasets-bucket/USER_Model/'
storage_integration = S3_external_table
file_format = User_MODEL_FILE;



create or replace stage external_stage_SHIPMENTS
url = 's3://ecommerce-datasets-bucket/SHIPMENTS_Model/'
storage_integration = S3_external_table
file_format = Shipments_MODEL_FILE;
// ===============================================================================================================================================
-- Creating Snowpipe and error Notification 
// ===============================================================================================================================================
show file formats in ecommerce.raw;
create  NOTIFICATION integration user_raw_email_notication
  TYPE = QUEUE
  ENABLED = TRUE
  DIRECTION = OUTBOUND
  NOTIFICATION_PROVIDER = AWS_SNS
  AWS_SNS_TOPIC_ARN = 'arn:aws:sns:ap-south-1:625693793411:Raw_User_table_notification'
  AWS_SNS_ROLE_ARN = 'arn:aws:iam::625693793411:role/S3_To_snowFlake'; 

DESC NOTIFICATION INTEGRATION user_raw_email_notication;

create or replace pipe raw_user_pipe1
  auto_ingest = TRUE
  as
  copy into RAW.USER_MODEL from @raw.external_stage_USER
  file_format = (format_name = 'raw.USER_MODEL_file')
    on_error = SKIP_FILE;
    
alter pipe raw_user_pipe1
set error_integration = user_raw_email_notication;
-----------------------------------------------------------------------------------------------------------
create or replace pipe raw_cart_pipe1
  auto_ingest = TRUE
  as
  copy into RAW.cart_MODEL from @raw.external_stage_Cart
  file_format = (format_name = 'raw.CART_MODEL_FILE')
    on_error = SKIP_FILE;

alter pipe raw_cart_pipe1
set error_integration = user_raw_email_notication;
------------------------------------------------------------------------------------------------------------
create or replace pipe raw_PAYMENTS_pipe1
  auto_ingest = TRUE
  as
  copy into RAW.payments_MODEL from @raw.external_stage_PAYMENTS
  file_format = (format_name = 'raw.PAYMENTS_MODEL_FILE')
    on_error = SKIP_FILE;

alter pipe raw_PAYMENTS_pipe1
set error_integration = user_raw_email_notication;
----------------------------------------------------------------------------------------------------
create or replace pipe raw_product_pipe1
  auto_ingest = TRUE
  as
  copy into RAW.product_MODEL from @raw.external_stage_product
  file_format = (format_name = 'raw.PRODUCT_MODEL_FILE')
    on_error = SKIP_FILE;

alter pipe raw_product_pipe1
set error_integration = user_raw_email_notication;
---------------------------------------------------------------------------------------------------------
create or replace pipe raw_RETURN_REFUND_pipe1
  auto_ingest = TRUE
  as
  copy into RAW.RETURN_REFUND_MODEL from @raw.external_stage_RETURN_REFUND
  file_format = (format_name = 'raw.RETURN_REFUND_FILE')
    on_error = SKIP_FILE;

alter pipe raw_RETURN_REFUND_pipe1
set error_integration = user_raw_email_notication;
------------------------------------------------------------------------------------------------------------
create or replace pipe raw_Seller_pipe1
  auto_ingest = TRUE
  as
  copy into RAW.seller_MODEL from @raw.external_stage_seller
  file_format = (format_name = 'raw.SELLER_MODEL_FILE')
    on_error = SKIP_FILE;


alter pipe raw_seller_pipe1
set error_integration = user_raw_email_notication;
------------------------------------------------------------------------------------------------------------
create or replace pipe raw_SHIPMENTS_pipe1
  auto_ingest = TRUE
  as
  copy into RAW.shipments_MODEL from @raw.external_stage_shipments
  file_format = (format_name = 'raw.SHIPMENTS_MODEL_FILE')
    on_error = SKIP_FILE;

alter pipe raw_shipments_pipe1
set error_integration = user_raw_email_notication;
------------------------------------------------------------------------------------------------------------
create or replace pipe raw_order_pipe1
  auto_ingest = TRUE
  as
  copy into RAW.order_MODEL from @raw.external_stage_order
  file_format = (format_name = 'raw.order_MODEL_FILE')
    on_error = SKIP_FILE;


alter pipe raw_order_pipe1
set error_integration = user_raw_email_notication;
// ===============================================================================================================================================
-- Raw_table(raw_streams) to Clean_table by using clean PROCEDURE
// ===============================================================================================================================================

show streams in ecommerce.raw;
CREATE OR REPLACE TASK CLEAN_USER_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.RAW_STREAM_USER')
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
AS
    CALL ECOMMERCE.clean.CLEAN_USER_PROCEDURE();

CREATE OR REPLACE TASK CLEAN_CART_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.Raw_STREAM_CART')
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
AS
    CALL ecommerce.clean.CLEAN_CART_PROCEDURE();

CREATE OR REPLACE TASK CLEAN_ORDER_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.Raw_STREAM_ORDER')
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
AS
    CALL ecommerce.clean.CLEAN_ORDER_PROCEDURE();

CREATE OR REPLACE TASK CLEAN_STREAM_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.Raw_STREAM_PAYMENTS')
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
AS
    CALL ecommerce.clean.CLEAN_PAYMENT_PROCEDURE();
    
CREATE OR REPLACE TASK CLEAN_PRODUCT_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.Raw_STREAM_PRODUCT')
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
AS
    CALL ecommerce.clean.CLEAN_PRODUCT_PROCEDURE();

CREATE OR REPLACE TASK CLEAN_RETURN_REFUND_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.Raw_STREAM_RETURN_REFUND')
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
AS
    CALL ecommerce.clean.CLEAN_RETURN_PROCEDURE();

CREATE OR REPLACE TASK CLEAN_SELLER_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.Raw_STREAM_SELLER')
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
AS
    CALL  ecommerce.clean.CLEAN_SELLER_PROCEDURE();

CREATE OR REPLACE TASK CLEAN_SHIPMENTS_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW.Raw_STREAM_SHIPMENTS')
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
AS
    CALL ecommerce.clean.CLEAN_SHIPMENT_PROCEDURE();
// ===============================================================================================================================================
-- clean_table to dim&fact
// ===============================================================================================================================================

create or replace task user_mart
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '15 MINUTE'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
    after ecommerce.clean.clean_user_task
As
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_USER();
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_ROLE();
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_DEVICE();
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_DATE();
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_TIME();
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_IP();
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_LOGIN_STATUS();
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_ACTIVITY();
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_NOTIFICATION_TYPE();

    -- Step 2: Dependent Dimensions & Bridge Tables
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_ADDRESS();
    CALL ECOMMERCE.CLEAN.SP_LOAD_DIM_MOBILE();
    CALL ECOMMERCE.CLEAN.SP_LOAD_BRIDGE_USR_ROLE();

    -- Step 3: Fact Tables
    CALL ECOMMERCE.CLEAN.SP_LOAD_FACT_LOGIN();
    CALL ECOMMERCE.CLEAN.SP_LOAD_FACT_ACTIVITY();
    CALL ECOMMERCE.CLEAN.SP_LOAD_FACT_NOTIFICATION();


    ALTER TASK CLEAN_USER_TASK RESUME





create or replace task  seller_mart
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
    after ecommerce.clean.clean_seller_task
As

create or replace task cart_mart
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
    after ecommerce.clean.clean_cart_task
As


create or replace task product_mart
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
    after ecommerce.clean.clean_product_task
As


create or replace task shipment_mart
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
    after ecommerce.clean.clean_shipment_task
As


create or replace task order_mart
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
    after ecommerce.clean.clean_order_task
As

create or replace task CLEAN_PAYMENTS_mart
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
    after ecommerce.clean.CLEAN_PAYMENTS_task
As

create or replace task CLEAN_RETURN_REFUND_mart
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    SUSPEND_TASK_AFTER_NUM_FAILURES = 3
    ERROR_INTEGRATION = user_raw_email_notication
    after ecommerce.clean.CLEAN_RETURN_REFUND_MODEL_task
As

