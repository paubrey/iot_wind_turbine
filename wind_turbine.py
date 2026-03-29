#!/usr/bin/env python3
"""
IoT Wind Turbine — Raspberry Pi Hardware Script
=================================================
Runs on a Raspberry Pi connected to a physical wind turbine rig. Reads sensor
data from an ADC (motor potentiometer on channel 0, photoresistor on channel 1),
drives a DC motor via an L293D H-bridge, collects Pi system metrics (CPU temp,
memory, disk, uptime, etc.), and streams every reading to Snowflake in real time
using the Snowpipe Streaming high-performance SDK.

Hardware requirements:
    - Raspberry Pi (tested on Pi 4 Model B)
    - ADC chip: PCF8591 (I2C addr 0x48) or ADS7830 (I2C addr 0x4b)
    - L293D motor driver on GPIO pins 17, 22, 27
    - Potentiometer on ADC channel 0 (motor speed/direction)
    - Photoresistor on ADC channel 1 (ambient light / solar proxy)

Configuration:
    All Snowflake connection details and device settings are read from a TOML
    config file. By default the script looks for `tgt_snf_account.toml` in the
    same directory, but you can override with the CONFIG_PATH env var.

    Required TOML sections:
        [snowflake]            — account, user, role, private_key_file
        [snowflake.target]     — database, schema, table
        [device]               — device_id, stream_interval

    Key-pair authentication is required. The scripts authenticate as the
    IOT_STREAMING_USER service user (TYPE=SERVICE, no password). Run setup.sql
    to create the user, role, and grants. Generate an RSA key pair:
        openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
        openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
    Then set the public key on the user (setup.sql has the placeholder).

Environment variable overrides:
    CONFIG_PATH       — path to the TOML config file
    DEVICE_ID         — override device identifier
    STREAM_INTERVAL   — seconds between readings (float)

Usage:
    # Run directly on the Pi:
    python3 wind_turbine.py

    # Override config path and interval:
    CONFIG_PATH=/path/to/config.toml STREAM_INTERVAL=0.5 python3 wind_turbine.py

    # Stop gracefully with Ctrl-C; remaining data is flushed before exit.

Snowflake target:
    Data lands in IOT_WIND_TURBINE.RAW.SENSOR_READINGS as a single VARIANT
    column (SENSOR_DATA_JSON) using the default streaming pipe with
    MATCH_BY_COLUMN_NAME. Query the typed view at:
        SELECT * FROM IOT_WIND_TURBINE.SILVER.SENSOR_READINGS;

Dependencies:
    pip install snowpipe-streaming gpiozero
    (ADCDevice.py must be in the same directory or on PYTHONPATH)
"""
import os
import sys
import time
import uuid
import socket
from datetime import datetime, timezone
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from gpiozero import DigitalOutputDevice, PWMOutputDevice  # Pi GPIO motor control
from ADCDevice import *  # I2C ADC helper (PCF8591 / ADS7830)
from snowflake.ingest.streaming import StreamingIngestClient  # Snowpipe Streaming HP SDK

# ── Configuration ────────────────────────────────────────────────────────────
# Load Snowflake connection details and device settings from TOML config.
CONFIG_PATH = os.getenv("CONFIG_PATH", str(Path(__file__).parent / "tgt_snf_account.toml"))

with open(CONFIG_PATH, "rb") as f:
    _cfg = tomllib.load(f)

_sf = _cfg["snowflake"]
_tgt = _sf["target"]
_dev = _cfg["device"]

SNOWFLAKE_ACCOUNT = _sf["account"]
SNOWFLAKE_USER = _sf["user"]
SNOWFLAKE_ROLE = _sf["role"]
SNOWFLAKE_PRIVATE_KEY_FILE = _sf["private_key_file"]
SNOWFLAKE_DATABASE = _tgt["database"]
SNOWFLAKE_SCHEMA = _tgt["schema"]
SNOWFLAKE_TABLE = _tgt["table"]
SNOWFLAKE_PIPE = f"{SNOWFLAKE_TABLE}-STREAMING"  # Default pipe auto-created by Snowflake

DEVICE_ID = os.getenv("DEVICE_ID", _dev["device_id"])
CHANNEL_NAME = f"{DEVICE_ID}-channel"
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", str(_dev["stream_interval"])))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

# ── GPIO & ADC Hardware Setup ────────────────────────────────────────────────
# L293D H-bridge motor driver pins
motoRPin1 = DigitalOutputDevice(27)   # direction pin A
motoRPin2 = DigitalOutputDevice(17)   # direction pin B
# enablePin = PWMOutputDevice(22, frequency=1000)  # PWM speed control
enablePin = PWMOutputDevice(12, frequency=1000)  # PWM speed control
adc = ADCDevice()  # auto-detected I2C ADC (PCF8591 or ADS7830)


# ── Sensor Functions ─────────────────────────────────────────────────────────

def setup_adc():
    global adc
    if adc.detectI2C(0x48):
        adc = PCF8591()
    elif adc.detectI2C(0x4b):
        adc = ADS7830()
    else:
        print("No correct I2C address found, \n"
              "Please use command 'i2cdetect -y 1' to check the I2C address! \n"
              "Program Exit. \n")
        exit(-1)


def mapNUM(value, fromLow, fromHigh, toLow, toHigh):  # Arduino-style range mapping
    return (toHigh - toLow) * (value - fromLow) / (fromHigh - fromLow) + toLow


def get_motor_state(adc_value):  # Translate ADC midpoint (128) into direction + duty cycle
    value = adc_value - 128
    if value > 0:
        direction = "forward"
    elif value < 0:
        direction = "backward"
    else:
        direction = "stopped"
    duty_cycle = abs(value) * 100 / 127
    return value, direction, duty_cycle


def drive_motor(adc_value):  # Set H-bridge direction pins and PWM duty cycle from ADC reading
    value, direction, duty_cycle = get_motor_state(adc_value)
    if value > 0:
        motoRPin1.on()
        motoRPin2.off()
    elif value < 0:
        motoRPin1.off()
        motoRPin2.on()
    else:
        motoRPin1.off()
        motoRPin2.off()
    b = mapNUM(abs(value), 0, 128, 0, 100)
    enablePin.value = b / 100.0
    return direction, duty_cycle


def get_photoresistor_reading():  # Read ambient light level from photoresistor on ADC ch1
    light_value = adc.analogRead(1)
    voltage = light_value / 255.0 * 3.3
    return light_value, voltage


def get_pi_system_metrics():  # Collect CPU temp, memory, disk, CPU%, serial, hostname, IP, uptime from /proc & /sys
    metrics = {}

    try:
        with open("/sys/class/thermal/thermal_zone0/temp", "r") as f:
            metrics["cpu_temperature_celsius"] = float(f.read().strip()) / 1000.0
    except Exception:
        metrics["cpu_temperature_celsius"] = None

    try:
        with open("/proc/meminfo", "r") as f:
            meminfo = {}
            for line in f:
                parts = line.split()
                meminfo[parts[0].rstrip(":")] = int(parts[1])
            total = meminfo.get("MemTotal", 0) / 1024
            free = meminfo.get("MemAvailable", meminfo.get("MemFree", 0)) / 1024
            used = total - free
            metrics["memory_total_mb"] = round(total, 1)
            metrics["memory_used_mb"] = round(used, 1)
            metrics["memory_free_mb"] = round(free, 1)
            metrics["memory_percent_used"] = round((used / total) * 100, 1) if total > 0 else 0
    except Exception:
        metrics["memory_total_mb"] = None
        metrics["memory_used_mb"] = None
        metrics["memory_free_mb"] = None
        metrics["memory_percent_used"] = None

    try:
        statvfs = os.statvfs("/")
        total = (statvfs.f_frsize * statvfs.f_blocks) / (1024 ** 3)
        free = (statvfs.f_frsize * statvfs.f_bavail) / (1024 ** 3)
        used = total - free
        metrics["disk_total_gb"] = round(total, 2)
        metrics["disk_used_gb"] = round(used, 2)
        metrics["disk_free_gb"] = round(free, 2)
        metrics["disk_percent_used"] = round((used / total) * 100, 1) if total > 0 else 0
    except Exception:
        metrics["disk_total_gb"] = None
        metrics["disk_used_gb"] = None
        metrics["disk_free_gb"] = None
        metrics["disk_percent_used"] = None

    try:
        with open("/proc/stat", "r") as f:
            line = f.readline()
            fields = line.strip().split()[1:]
            idle = int(fields[3])
            total_cpu = sum(int(x) for x in fields)
        time.sleep(0.05)
        with open("/proc/stat", "r") as f:
            line = f.readline()
            fields = line.strip().split()[1:]
            idle2 = int(fields[3])
            total_cpu2 = sum(int(x) for x in fields)
        idle_delta = idle2 - idle
        total_delta = total_cpu2 - total_cpu
        metrics["cpu_percent"] = round((1.0 - idle_delta / total_delta) * 100, 1) if total_delta > 0 else 0
    except Exception:
        metrics["cpu_percent"] = None

    try:
        with open("/proc/cpuinfo", "r") as f:
            serial = None
            model = None
            for line in f:
                if line.startswith("Serial"):
                    serial = line.strip().split(":")[1].strip()
                if line.startswith("Model"):
                    model = line.strip().split(":")[1].strip()
            metrics["pi_serial_number"] = serial
            metrics["pi_model"] = model
    except Exception:
        metrics["pi_serial_number"] = None
        metrics["pi_model"] = None

    try:
        metrics["hostname"] = socket.gethostname()
    except Exception:
        metrics["hostname"] = None

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        metrics["ip_address"] = s.getsockname()[0]
        s.close()
    except Exception:
        metrics["ip_address"] = None

    try:
        with open("/proc/uptime", "r") as f:
            metrics["uptime_seconds"] = float(f.read().split()[0])
    except Exception:
        metrics["uptime_seconds"] = None

    return metrics


# ── Snowflake Streaming ──────────────────────────────────────────────────────

def create_streaming_client():  # Open Snowpipe Streaming channel using key-pair auth
    client = StreamingIngestClient(
        client_name=f"{DEVICE_ID}-client",
        db_name=SNOWFLAKE_DATABASE,
        schema_name=SNOWFLAKE_SCHEMA,
        pipe_name=SNOWFLAKE_PIPE,
        properties={
            "account": SNOWFLAKE_ACCOUNT,
            "user": SNOWFLAKE_USER,
            "private_key_file": SNOWFLAKE_PRIVATE_KEY_FILE,
            "role": SNOWFLAKE_ROLE,
            "url": f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com",
        },
    )
    channel, status = client.open_channel(CHANNEL_NAME)
    print(f"Streaming channel opened: {CHANNEL_NAME} (status: {status})")
    return client, channel


def build_row(motor_adc, direction, duty_cycle, light_adc, light_voltage, pi_metrics):  # Assemble JSON payload for one sensor reading
    payload = {
        "reading_id": str(uuid.uuid4()),
        "device_id": DEVICE_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "turbine_data": {
            "motor_adc_value": motor_adc,
            "motor_adc_voltage": round(motor_adc / 255.0 * 3.3, 4),
            "motor_direction": direction,
            "motor_pwm_duty_cycle": round(duty_cycle, 2),
            "estimated_power_watts": round(
                (motor_adc / 255.0 * 3.3) * (duty_cycle / 100.0) * 0.5, 4
            ),
        },
        "solar_data": {
            "photoresistor_adc_value": light_adc,
            "photoresistor_voltage": round(light_voltage, 4),
        },
        "system_data": pi_metrics,
    }
    return {"sensor_data_json": payload}  # Single-key dict maps to VARIANT column via MATCH_BY_COLUMN_NAME


# ── Main Loop ────────────────────────────────────────────────────────────────

def loop(channel):  # Infinite read-drive-stream loop; Ctrl-C to stop
    seq = 0
    batch = []
    while True:
        motor_adc = adc.analogRead(0)
        direction, duty_cycle = drive_motor(motor_adc)

        light_adc, light_voltage = get_photoresistor_reading()

        pi_metrics = get_pi_system_metrics()

        row = build_row(motor_adc, direction, duty_cycle, light_adc, light_voltage, pi_metrics)
        batch.append(row)
        seq += 1

        if len(batch) >= BATCH_SIZE:
            channel.append_rows(batch)
            print(f"[{seq}] Sent batch of {len(batch)} | "
                  f"Motor: {motor_adc} ({direction}, {duty_cycle:.0f}%) | "
                  f"Light: {light_adc} ({light_voltage:.2f}V) | "
                  f"CPU: {pi_metrics.get('cpu_temperature_celsius', '?')}°C | "
                  f"Mem: {pi_metrics.get('memory_percent_used', '?')}%")
            batch = []

        time.sleep(STREAM_INTERVAL)


def destroy(client, channel):  # Graceful shutdown: release GPIO, flush & close Snowflake channel
    motoRPin1.close()
    motoRPin2.close()
    enablePin.close()
    adc.close()
    print("Flushing remaining data to Snowflake...")
    channel.close(wait_for_flush=True, timeout_seconds=30)
    client.close()
    print("Streaming client closed.")


if __name__ == "__main__":
    print("Program is starting ... ")
    setup_adc()
    client, channel = create_streaming_client()
    try:
        loop(channel)
    except KeyboardInterrupt:
        destroy(client, channel)
        print("Ending program")
