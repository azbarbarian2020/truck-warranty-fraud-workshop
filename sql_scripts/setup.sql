-- ═══════════════════════════════════════════════════════════════════
-- TRUCK WARRANTY FRAUD DETECTION WORKSHOP - SETUP SCRIPT
-- ═══════════════════════════════════════════════════════════════════
-- 
-- This script sets up the complete data environment for the workshop.
-- 
-- INSTRUCTIONS:
-- 1. Edit the database and schema names below (lines 14-15)
-- 2. Upload CSV files to DATA_FILES stage (via Snowflake UI)
-- 3. Upload PDF files to DOCUMENTS stage (via Snowflake UI)
-- 4. Run this entire script
--
-- Time: 15-20 minutes
-- ═══════════════════════════════════════════════════════════════════

-- ┌─────────────────────────────────────────────────────────────────┐
-- │ CONFIGURATION - EDIT THESE VALUES                               │
-- └─────────────────────────────────────────────────────────────────┘

SET DATABASE_NAME = 'TRAINING_DB';  
--SET SCHEMA_NAME = '<YOUR WiW>_2025_SF_LAB'; -- e.g. BEACHAC_2025_SF_LAB
SET SCHEMA_NAME = 'BEACHAC_DD_2025_SF_LAB';  -- cb note: use a new schema as when I tried to resuse my old one I had a role / permissions clash               
SET WAREHOUSE_NAME = 'SSZ_TRAINING_ADHOC_WH';               

-- ═══════════════════════════════════════════════════════════════════
-- STEP 1: CREATE DATABASE, SCHEMA, AND STAGES
-- ═══════════════════════════════════════════════════════════════════

USE ROLE SSZ_TRAINING_FR;
USE WAREHOUSE SSZ_TRAINING_ADHOC_WH;

-- CREATE DATABASE IF NOT EXISTS IDENTIFIER($DATABASE_NAME); --cb note: dtna users do not have permission
USE DATABASE IDENTIFIER($DATABASE_NAME);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($SCHEMA_NAME);
USE SCHEMA IDENTIFIER($SCHEMA_NAME);

USE WAREHOUSE IDENTIFIER($WAREHOUSE_NAME);

-- Create stages for file uploads
CREATE STAGE IF NOT EXISTS DATA_FILES
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  COMMENT = 'Stage for CSV data files';

CREATE STAGE IF NOT EXISTS DOCUMENTS
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  COMMENT = 'Stage for PDF documents';

SHOW STAGES;

SELECT '✓ Step 1 Complete: Database, schema, and stages created' as STATUS;
SELECT 'Database: ' || $DATABASE_NAME as INFO;
SELECT 'Schema: ' || $SCHEMA_NAME as INFO;

-- ═══════════════════════════════════════════════════════════════════
-- STEP 2: CREATE TABLES
-- ═══════════════════════════════════════════════════════════════════

-- Dimension: Dealers
CREATE TABLE IF NOT EXISTS DEALERS (
    DEALER_ID VARCHAR(20) PRIMARY KEY,
    DEALER_NAME VARCHAR(200),
    REGION VARCHAR(50)
);

-- Dimension: Vehicles
CREATE TABLE IF NOT EXISTS VEHICLES (
    CHASSIS_NUMBER VARCHAR(20) PRIMARY KEY,
    DEALER_ID VARCHAR(20),
    MODEL_NAME VARCHAR(50),
    MODEL_CATEGORY VARCHAR(50),
    ENGINE_TYPE VARCHAR(50),
    FUEL_TYPE VARCHAR(50),
    WARRANTY_START_DATE DATE,
    WARRANTY_END_DATE DATE
);

-- Dimension: Technicians
CREATE TABLE IF NOT EXISTS TECHNICIANS (
    TECHNICIAN_ID VARCHAR(20) PRIMARY KEY,
    TECHNICIAN_NAME VARCHAR(200),
    SPECIALIZATION VARCHAR(200),
    CERTIFICATION_LEVEL VARCHAR(200),
    YEARS_EXPERIENCE NUMBER(3,0)
);

-- Fact: Sales
CREATE TABLE IF NOT EXISTS SALES (
    CHASSIS_NUMBER VARCHAR(20),
    SALES_DEALER_ID VARCHAR(20),
    SALE_DATE DATE,
    SALE_PRICE NUMBER(12,2)
);

-- Fact: Service (warranty, maintenance, repairs)
CREATE TABLE IF NOT EXISTS SERVICE (
    SERVICE_ID NUMBER PRIMARY KEY,
    CHASSIS_NUMBER VARCHAR(20),
    SERVICE_DEALER_ID VARCHAR(20),
    TECHNICIAN_ID VARCHAR(20),
    SERVICE_DATE DATE,
    SERVICE_TYPE VARCHAR(20),
    SERVICE_AMOUNT NUMBER(12,2),
    PARTS_REPLACED VARCHAR(500),
    TECHNICIAN_NOTES VARCHAR(5000),
    FAULT_CODE VARCHAR(20),
    CLAIM_STATUS VARCHAR(50),
    WARRANTY_TYPE VARCHAR(50),
    CUSTOMER_PAY VARCHAR(10),
    MILEAGE NUMBER(10,0)
);

SHOW TABLES;

SELECT '✓ Step 2 Complete: Tables created' as STATUS;

-- ═══════════════════════════════════════════════════════════════════
-- PAUSE HERE: UPLOAD FILES VIA SNOWFLAKE UI
-- ═══════════════════════════════════════════════════════════════════
-- 
-- Before continuing, upload files via Snowflake UI:
--
-- 1. Go to: Data > Databases > [YOUR_DATABASE] > [YOUR_SCHEMA] > Stages
-- 
-- 2. Click on DATA_FILES stage, then "+ Files" button
--    Upload these CSV files:
--    - dealers.csv
--    - vehicles.csv
--    - technicians.csv
--    - sales.csv
--    - service.csv (or service_0_0_0.csv, service_0_0_1.csv, etc.)
--
-- 3. Click on DOCUMENTS stage, then "+ Files" button
--    Upload these PDF files:
--    - MODEL-T2000_Service_Guide.pdf
--    - MODEL-T3000_Service_Guide.pdf
--    - MODEL-T4000_Service_Guide.pdf
--    - MODEL-T5000_Service_Guide.pdf
--    - MODEL-T6000_Service_Guide.pdf
--    - MODEL-T7000_Service_Guide.pdf
--    - MODEL-T8000_Service_Guide.pdf
--    - MODEL-T9000_Service_Guide.pdf
--    - WARRANTY_POLICY.pdf
--
-- 4. Verify uploads by running:
--
-- ═══════════════════════════════════════════════════════════════════

LIST @DATA_FILES;
LIST @DOCUMENTS;

-- You should see 5+ CSV files and 9 PDF files
-- If not, upload them before continuing

SELECT '✓ Files uploaded - ready to load data' as STATUS;

-- ═══════════════════════════════════════════════════════════════════
-- STEP 3: LOAD DATA INTO TABLES
-- ═══════════════════════════════════════════════════════════════════

-- Load DEALERS
COPY INTO DEALERS
FROM @DATA_FILES/dealers.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT '✓ Loaded ' || COUNT(*) || ' dealers (expected: 100)' as STATUS FROM DEALERS;

-- Load VEHICLES
COPY INTO VEHICLES
FROM @DATA_FILES/vehicles.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT '✓ Loaded ' || COUNT(*) || ' vehicles (expected: 29,968)' as STATUS FROM VEHICLES;

-- Load TECHNICIANS
COPY INTO TECHNICIANS
FROM @DATA_FILES/technicians.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT '✓ Loaded ' || COUNT(*) || ' technicians (expected: 560)' as STATUS FROM TECHNICIANS;

-- Load SALES
COPY INTO SALES
FROM @DATA_FILES/sales.csv
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT '✓ Loaded ' || COUNT(*) || ' sales (expected: 29,968)' as STATUS FROM SALES;

-- Load SERVICE (handles multiple files if split)
COPY INTO SERVICE
FROM @DATA_FILES/
PATTERN = '.*service.*[.]csv'
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    ESCAPE = '\\'
    ESCAPE_UNENCLOSED_FIELD = '\\'
)
ON_ERROR = 'ABORT_STATEMENT';

SELECT '✓ Loaded ' || COUNT(*) || ' service records (expected: 67,011)' as STATUS FROM SERVICE;

-- Summary
SELECT 
    'DATA LOAD SUMMARY' as REPORT,
    (SELECT COUNT(*) FROM DEALERS) as DEALERS,
    (SELECT COUNT(*) FROM VEHICLES) as VEHICLES,
    (SELECT COUNT(*) FROM TECHNICIANS) as TECHNICIANS,
    (SELECT COUNT(*) FROM SALES) as SALES,
    (SELECT COUNT(*) FROM SERVICE) as SERVICE_RECORDS;

SELECT '✓ Step 3 Complete: All data loaded' as STATUS;

-- ═══════════════════════════════════════════════════════════════════
-- STEP 4: VALIDATION
-- ═══════════════════════════════════════════════════════════════════

SELECT '════════════════════════════════════════════════════════════════' as VALIDATION;
SELECT 'SETUP VALIDATION' as VALIDATION;
SELECT '════════════════════════════════════════════════════════════════' as VALIDATION;

-- Check 1: Row Counts
SELECT 
    'DEALERS' as TABLE_NAME,
    COUNT(*) as ROW_COUNT,
    CASE WHEN COUNT(*) = 100 THEN '✓ Expected' ELSE '⚠ Check count' END as STATUS
FROM DEALERS
UNION ALL
SELECT 'VEHICLES', COUNT(*), CASE WHEN COUNT(*) = 29968 THEN '✓ Expected' ELSE '⚠ Check count' END FROM VEHICLES
UNION ALL
SELECT 'TECHNICIANS', COUNT(*), CASE WHEN COUNT(*) = 560 THEN '✓ Expected' ELSE '⚠ Check count' END FROM TECHNICIANS
UNION ALL
SELECT 'SALES', COUNT(*), CASE WHEN COUNT(*) = 29968 THEN '✓ Expected' ELSE '⚠ Check count' END FROM SALES
UNION ALL
SELECT 'SERVICE', COUNT(*), CASE WHEN COUNT(*) = 67011 THEN '✓ Expected' ELSE '⚠ Check count' END FROM SERVICE;

-- Check 2: Warranty Summary
SELECT 
    COUNT(*) as TOTAL_WARRANTY_CLAIMS,
    TO_CHAR(SUM(SERVICE_AMOUNT), '$999,999,999') as TOTAL_WARRANTY_COST,
    TO_CHAR(AVG(SERVICE_AMOUNT), '$999,999') as AVG_CLAIM_COST
FROM SERVICE
WHERE SERVICE_TYPE = 'WARRANTY';

-- Expected: Check actual values from your data

-- Check 3: Top Dealers (Should see outliers)
WITH sales_by_dealer AS (
    SELECT 
        d.DEALER_ID,
        d.DEALER_NAME,
        COALESCE(SUM(s.SALE_PRICE), 0) as sales_revenue
    FROM DEALERS d
    LEFT JOIN SALES s ON d.DEALER_ID = s.SALES_DEALER_ID
    GROUP BY d.DEALER_ID, d.DEALER_NAME
),
service_by_dealer AS (
    SELECT 
        d.DEALER_ID,
        COALESCE(SUM(CASE WHEN srv.SERVICE_TYPE = 'WARRANTY' THEN srv.SERVICE_AMOUNT ELSE 0 END), 0) as warranty_costs,
        COALESCE(SUM(CASE WHEN srv.SERVICE_TYPE = 'MAINTENANCE' THEN srv.SERVICE_AMOUNT ELSE 0 END), 0) as maintenance_revenue,
        COALESCE(SUM(CASE WHEN srv.SERVICE_TYPE = 'REPAIR' THEN srv.SERVICE_AMOUNT ELSE 0 END), 0) as repair_revenue
    FROM DEALERS d
    LEFT JOIN SERVICE srv ON d.DEALER_ID = srv.SERVICE_DEALER_ID
    GROUP BY d.DEALER_ID
)
SELECT 
    sa.DEALER_NAME,
    TO_CHAR(sv.warranty_costs, '$999,999,999') as WARRANTY_COSTS,
    ROUND((sv.warranty_costs / NULLIF(sa.sales_revenue + sv.maintenance_revenue + sv.repair_revenue, 0)) * 100, 2) || '%' as WARRANTY_PCT,
    CASE 
        WHEN (sv.warranty_costs / NULLIF(sa.sales_revenue + sv.maintenance_revenue + sv.repair_revenue, 0)) * 100 > 6 
        THEN '🚨 High Outlier'
        WHEN (sv.warranty_costs / NULLIF(sa.sales_revenue + sv.maintenance_revenue + sv.repair_revenue, 0)) * 100 > 4 
        THEN '⚠ Elevated'
        ELSE '✓ Normal'
    END as ASSESSMENT
FROM sales_by_dealer sa
JOIN service_by_dealer sv ON sa.DEALER_ID = sv.DEALER_ID
ORDER BY sv.warranty_costs DESC
LIMIT 5;

-- Expected: Lewis Commercial Sales and Anderson Truck Center as outliers (6-7.5%)

SELECT '════════════════════════════════════════════════════════════════' as VALIDATION;
SELECT '✓ VALIDATION COMPLETE - DATA SETUP READY!' as VALIDATION;
SELECT '════════════════════════════════════════════════════════════════' as VALIDATION;

-- ═══════════════════════════════════════════════════════════════════
-- SETUP COMPLETE!
-- ═══════════════════════════════════════════════════════════════════
--
-- Your data is now ready for the workshop.
--
-- Next steps:
-- 1. Create Cortex Search services (via Snowflake UI)
-- 2. Create Cortex Analyst service (via Snowflake UI)
-- 3. Create Intelligence Agent (via Snowflake UI)
-- 4. Run test queries to detect fraud
--
-- Expected result: $6.2M in suspicious claims detected
--
-- ═══════════════════════════════════════════════════════════════════

SELECT '🎉 Setup Complete! Ready for workshop!' as STATUS;
