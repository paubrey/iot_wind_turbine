-- =============================================================================
-- IoT Wind Turbine — Snowflake Setup
-- =============================================================================
-- Run this script once (as ACCOUNTADMIN or equivalent) to create:
--   1. The IOT_STREAMING_ROLE custom role (least-privilege for streaming)
--   2. The IOT_STREAMING_USER service user (key-pair auth only, no password)
--   3. The IOT_WIND_TURBINE database with RAW and SILVER schemas
--   4. The landing table and typed reporting view
--   5. All necessary grants
--
-- Prerequisites:
--   1. You must run this as a role that can create users, roles, databases
--      (ACCOUNTADMIN or USERADMIN + SYSADMIN).
--   2. Generate an RSA key pair before running:
--        openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--        openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--      Then paste the public key (without header/footer) into the placeholder below.
--
-- Architecture:
--   IOT_WIND_TURBINE
--   ├── RAW            — landing zone: raw JSON from Snowpipe Streaming
--   │   └── SENSOR_READINGS (VARIANT column)
--   └── SILVER         — typed reporting layer: one column per metric
--       └── SENSOR_READINGS (view over RAW.SENSOR_READINGS)
--
-- After setup, stream data with either Python script, then query:
--   SELECT * FROM IOT_WIND_TURBINE.SILVER.SENSOR_READINGS
--     ORDER BY READING_TIMESTAMP DESC LIMIT 20;
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- ── Custom Role ──────────────────────────────────────────────────────────────
-- Least-privilege role for Snowpipe Streaming ingestion only.

CREATE ROLE IF NOT EXISTS IOT_STREAMING_ROLE
    COMMENT = 'Least-privilege role for IoT wind turbine Snowpipe Streaming ingestion';

GRANT ROLE IOT_STREAMING_ROLE TO ROLE SYSADMIN;

-- ── Service User ─────────────────────────────────────────────────────────────
-- TYPE = SERVICE: non-human / programmatic user; cannot log in interactively.
-- Authenticates exclusively via RSA key-pair (no password).
--
-- Replace <your_rsa_public_key> with the contents of rsa_key.pub
-- (everything between BEGIN PUBLIC KEY / END PUBLIC KEY, on one line, no
-- header/footer).

CREATE USER IF NOT EXISTS IOT_STREAMING_USER
    TYPE = SERVICE
    DEFAULT_ROLE = IOT_STREAMING_ROLE
    DEFAULT_WAREHOUSE = COMPUTE_WH
    RSA_PUBLIC_KEY = '<your_rsa_public_key>'
    COMMENT = 'Service user for IoT wind turbine Snowpipe Streaming ingestion';

GRANT ROLE IOT_STREAMING_ROLE TO USER IOT_STREAMING_USER;

-- ── Database & Schemas ───────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS IOT_WIND_TURBINE;

CREATE SCHEMA IF NOT EXISTS IOT_WIND_TURBINE.RAW
    COMMENT = 'Raw landing zone for Snowpipe Streaming ingestion';

CREATE SCHEMA IF NOT EXISTS IOT_WIND_TURBINE.SILVER
    COMMENT = 'Typed reporting layer with strongly-typed columns extracted from raw JSON';

-- ── RAW table ────────────────────────────────────────────────────────────────
-- Single VARIANT column receives the full JSON payload from the Python scripts.
-- The Snowpipe Streaming default pipe uses MATCH_BY_COLUMN_NAME to map the
-- dict key "sensor_data_json" to this column automatically.

CREATE TABLE IF NOT EXISTS IOT_WIND_TURBINE.RAW.SENSOR_READINGS (
    SENSOR_DATA_JSON VARIANT
);

-- ── SILVER view ──────────────────────────────────────────────────────────────
-- Extracts and strongly types every field from the nested JSON into flat
-- columns for easy querying, dashboards, and downstream analytics.

CREATE OR REPLACE VIEW IOT_WIND_TURBINE.SILVER.SENSOR_READINGS (
    READING_ID,
    DEVICE_ID,
    READING_TIMESTAMP,
    MOTOR_ADC_VALUE,
    MOTOR_ADC_VOLTAGE,
    MOTOR_DIRECTION,
    MOTOR_PWM_DUTY_CYCLE,
    ESTIMATED_POWER_WATTS,
    PHOTORESISTOR_ADC_VALUE,
    PHOTORESISTOR_VOLTAGE,
    CPU_TEMPERATURE_CELSIUS,
    MEMORY_TOTAL_MB,
    MEMORY_USED_MB,
    MEMORY_FREE_MB,
    MEMORY_PERCENT_USED,
    DISK_TOTAL_GB,
    DISK_USED_GB,
    DISK_FREE_GB,
    DISK_PERCENT_USED,
    CPU_PERCENT,
    PI_SERIAL_NUMBER,
    PI_MODEL,
    HOSTNAME,
    IP_ADDRESS,
    UPTIME_SECONDS
) AS
SELECT
    SENSOR_DATA_JSON:reading_id::STRING                            AS READING_ID,
    SENSOR_DATA_JSON:device_id::STRING                             AS DEVICE_ID,
    SENSOR_DATA_JSON:timestamp::TIMESTAMP_NTZ                      AS READING_TIMESTAMP,
    SENSOR_DATA_JSON:turbine_data.motor_adc_value::NUMBER          AS MOTOR_ADC_VALUE,
    SENSOR_DATA_JSON:turbine_data.motor_adc_voltage::FLOAT         AS MOTOR_ADC_VOLTAGE,
    SENSOR_DATA_JSON:turbine_data.motor_direction::STRING           AS MOTOR_DIRECTION,
    SENSOR_DATA_JSON:turbine_data.motor_pwm_duty_cycle::FLOAT      AS MOTOR_PWM_DUTY_CYCLE,
    SENSOR_DATA_JSON:turbine_data.estimated_power_watts::FLOAT     AS ESTIMATED_POWER_WATTS,
    SENSOR_DATA_JSON:solar_data.photoresistor_adc_value::NUMBER    AS PHOTORESISTOR_ADC_VALUE,
    SENSOR_DATA_JSON:solar_data.photoresistor_voltage::FLOAT       AS PHOTORESISTOR_VOLTAGE,
    SENSOR_DATA_JSON:system_data.cpu_temperature_celsius::FLOAT    AS CPU_TEMPERATURE_CELSIUS,
    SENSOR_DATA_JSON:system_data.memory_total_mb::FLOAT            AS MEMORY_TOTAL_MB,
    SENSOR_DATA_JSON:system_data.memory_used_mb::FLOAT             AS MEMORY_USED_MB,
    SENSOR_DATA_JSON:system_data.memory_free_mb::FLOAT             AS MEMORY_FREE_MB,
    SENSOR_DATA_JSON:system_data.memory_percent_used::FLOAT        AS MEMORY_PERCENT_USED,
    SENSOR_DATA_JSON:system_data.disk_total_gb::FLOAT              AS DISK_TOTAL_GB,
    SENSOR_DATA_JSON:system_data.disk_used_gb::FLOAT               AS DISK_USED_GB,
    SENSOR_DATA_JSON:system_data.disk_free_gb::FLOAT               AS DISK_FREE_GB,
    SENSOR_DATA_JSON:system_data.disk_percent_used::FLOAT          AS DISK_PERCENT_USED,
    SENSOR_DATA_JSON:system_data.cpu_percent::FLOAT                AS CPU_PERCENT,
    SENSOR_DATA_JSON:system_data.pi_serial_number::STRING          AS PI_SERIAL_NUMBER,
    SENSOR_DATA_JSON:system_data.pi_model::STRING                  AS PI_MODEL,
    SENSOR_DATA_JSON:system_data.hostname::STRING                  AS HOSTNAME,
    SENSOR_DATA_JSON:system_data.ip_address::STRING                AS IP_ADDRESS,
    SENSOR_DATA_JSON:system_data.uptime_seconds::FLOAT             AS UPTIME_SECONDS
FROM IOT_WIND_TURBINE.RAW.SENSOR_READINGS;

-- ── Grants ───────────────────────────────────────────────────────────────────
-- Warehouse access (needed to execute queries / streaming)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE IOT_STREAMING_ROLE;

-- Database-level access
GRANT USAGE ON DATABASE IOT_WIND_TURBINE TO ROLE IOT_STREAMING_ROLE;

-- RAW schema: usage + insert (Snowpipe Streaming writes rows)
GRANT USAGE ON SCHEMA IOT_WIND_TURBINE.RAW TO ROLE IOT_STREAMING_ROLE;
GRANT INSERT, SELECT ON TABLE IOT_WIND_TURBINE.RAW.SENSOR_READINGS TO ROLE IOT_STREAMING_ROLE;

-- SILVER schema: usage + select (read-only reporting layer)
GRANT USAGE ON SCHEMA IOT_WIND_TURBINE.SILVER TO ROLE IOT_STREAMING_ROLE;
GRANT SELECT ON VIEW IOT_WIND_TURBINE.SILVER.SENSOR_READINGS TO ROLE IOT_STREAMING_ROLE;
