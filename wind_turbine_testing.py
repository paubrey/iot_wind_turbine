#!/usr/bin/env python3
"""
IoT Wind Turbine — Test Rig Simulator
=======================================
Generates realistic but fake sensor and system data and streams it to Snowflake
via the Snowpipe Streaming HP SDK. Use this script to populate the same
Snowflake table as the real Raspberry Pi hardware script (wind_turbine.py)
without needing any physical hardware.

Simulated data includes:
    - Motor ADC value, direction, PWM duty cycle, estimated power (sinusoidal)
    - Photoresistor ADC value and voltage (slow sinusoidal with noise)
    - Pi system metrics: CPU temp, memory, disk, CPU%, hostname, uptime, etc.

Configuration:
    Reads Snowflake connection and device settings from a TOML config file.
    By default it looks for `tgt_snf_account.toml` in the same directory;
    override with the CONFIG_PATH env var.

    Required TOML sections:
        [snowflake]            — account, user, role, private_key_file
        [snowflake.target]     — database, schema, table
        [device]               — stream_interval

    Key-pair authentication is required. The scripts authenticate as the
    IOT_STREAMING_USER service user (TYPE=SERVICE, no password). Run setup.sql
    to create the user, role, and grants. See wind_turbine.py docstring for
    key generation instructions.

Environment variable overrides:
    CONFIG_PATH       — path to the TOML config file
    DEVICE_ID         — identifier for the simulated device (default: "test-turbine-sim")
    STREAM_INTERVAL   — seconds between readings (float; default from config)
    NUM_READINGS      — total number of readings to generate (default: 500)

Usage:
    # Run with defaults (500 readings):
    python3 wind_turbine_testing.py

    # Custom number of readings and interval:
    NUM_READINGS=100 STREAM_INTERVAL=0.2 python3 wind_turbine_testing.py

    # Use a different config file:
    CONFIG_PATH=/path/to/config.toml python3 wind_turbine_testing.py

    # Stop early with Ctrl-C; remaining data is flushed before exit.

Snowflake target:
    Data lands in IOT_WIND_TURBINE.RAW.SENSOR_READINGS as a single VARIANT
    column (SENSOR_DATA_JSON). Query the typed view at:
        SELECT * FROM IOT_WIND_TURBINE.SILVER.SENSOR_READINGS;

Dependencies:
    pip install snowpipe-streaming
    (No GPIO or ADC hardware libraries needed — everything is simulated.)
"""
import os
import math
import time
import uuid
import random
import socket
import platform
from datetime import datetime, timezone
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

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

DEVICE_ID = os.getenv("DEVICE_ID", "test-turbine-sim")  # Distinguishes simulator rows from real Pi data
CHANNEL_NAME = f"{DEVICE_ID}-channel"
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", str(_dev["stream_interval"])))
NUM_READINGS = int(os.getenv("NUM_READINGS", "500"))  # Total simulated readings to generate


# ── Simulated Sensor Data ─────────────────────────────────────────────────────
# Generates realistic sinusoidal + noise sensor values that mimic a real
# wind turbine rig (motor ADC, photoresistor, Pi system metrics).

class SimulatedSensors:
    def __init__(self):
        self.pot_angle = 0.0
        self.light_level = 128
        self.time_offset = 0.0
        self.base_cpu_temp = 42.0
        self.base_mem_used = 450.0
        self.mem_total = 1024.0
        self.disk_total = 29.7
        self.disk_used = 8.5

    def tick(self):  # Advance simulation clock and recalculate sensor values
        self.time_offset += STREAM_INTERVAL
        self.pot_angle = 128 + 127 * math.sin(self.time_offset * 0.3)
        self.light_level = int(128 + 100 * math.sin(self.time_offset * 0.05 + 1.0))
        self.light_level = max(0, min(255, self.light_level + random.randint(-5, 5)))

    def read_motor_adc(self):  # Simulated ADC channel 0 (potentiometer controlling motor)
        return max(0, min(255, int(self.pot_angle + random.gauss(0, 2))))

    def read_photoresistor_adc(self):  # Simulated ADC channel 1 (ambient light sensor)
        return self.light_level

    def get_motor_state(self, adc_value):  # Translate ADC midpoint (128) into direction + duty cycle
        value = adc_value - 128
        if value > 0:
            direction = "forward"
        elif value < 0:
            direction = "backward"
        else:
            direction = "stopped"
        duty_cycle = abs(value) * 100 / 127
        return direction, duty_cycle

    def get_system_metrics(self):  # Return simulated Pi system metrics (CPU, memory, disk, uptime)
        cpu_temp = self.base_cpu_temp + 5 * math.sin(self.time_offset * 0.1) + random.gauss(0, 0.5)
        mem_used = self.base_mem_used + 50 * math.sin(self.time_offset * 0.02) + random.gauss(0, 10)
        mem_free = self.mem_total - mem_used
        cpu_pct = 15 + 10 * math.sin(self.time_offset * 0.15) + random.gauss(0, 2)
        uptime = 86400 + self.time_offset

        return {
            "cpu_temperature_celsius": round(cpu_temp, 1),
            "memory_total_mb": self.mem_total,
            "memory_used_mb": round(mem_used, 1),
            "memory_free_mb": round(mem_free, 1),
            "memory_percent_used": round((mem_used / self.mem_total) * 100, 1),
            "disk_total_gb": self.disk_total,
            "disk_used_gb": self.disk_used,
            "disk_free_gb": round(self.disk_total - self.disk_used, 2),
            "disk_percent_used": round((self.disk_used / self.disk_total) * 100, 1),
            "cpu_percent": round(max(0, min(100, cpu_pct)), 1),
            "pi_serial_number": "00000000aabbccdd",
            "pi_model": "Raspberry Pi 4 Model B Rev 1.4 (simulated)",
            "hostname": socket.gethostname(),
            "ip_address": "192.168.1.100",
            "uptime_seconds": round(uptime, 1),
        }


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


def build_row(device_id, motor_adc, direction, duty_cycle, light_adc, light_voltage, system_metrics):  # Assemble JSON payload for one sensor reading
    payload = {
        "reading_id": str(uuid.uuid4()),
        "device_id": device_id,
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
            "photoresistor_voltage": round(light_adc / 255.0 * 3.3, 4),
        },
        "system_data": system_metrics,
    }
    return {"sensor_data_json": payload}  # Single-key dict maps to VARIANT column via MATCH_BY_COLUMN_NAME


# ── Simulation Loop ──────────────────────────────────────────────────────────

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

def run_simulation(channel):  # Generate NUM_READINGS fake readings and stream in batches to Snowflake
    sim = SimulatedSensors()
    print(f"\nStreaming {NUM_READINGS} simulated readings to Snowflake (batch size {BATCH_SIZE})...\n")

    batch = []
    for seq in range(NUM_READINGS):
        sim.tick()
        motor_adc = sim.read_motor_adc()
        direction, duty_cycle = sim.get_motor_state(motor_adc)
        light_adc = sim.read_photoresistor_adc()
        light_voltage = light_adc / 255.0 * 3.3
        system_metrics = sim.get_system_metrics()

        row = build_row(DEVICE_ID, motor_adc, direction, duty_cycle, light_adc, light_voltage, system_metrics)
        batch.append(row)

        if len(batch) >= BATCH_SIZE or seq == NUM_READINGS - 1:
            channel.append_rows(batch)
            print(f"  [{seq + 1}/{NUM_READINGS}] Sent batch of {len(batch)} | "
                  f"Motor: {motor_adc} ({direction}, {duty_cycle:.0f}%) | "
                  f"Light: {light_adc} ({light_voltage:.2f}V) | "
                  f"CPU: {system_metrics['cpu_temperature_celsius']}°C")
            batch = []

        time.sleep(STREAM_INTERVAL)

    print(f"\nAll {NUM_READINGS} rows sent. Flushing...")
    channel.close(wait_for_flush=True, timeout_seconds=60)
    print("Flush complete. Data should be queryable in Snowflake within ~10 seconds.")


def main():
    print("=" * 60)
    print("  IoT Wind Turbine - Test Rig (Simulated Sensors)")
    print("=" * 60)
    print(f"  Config:       {CONFIG_PATH}")
    print(f"  Device ID:    {DEVICE_ID}")
    print(f"  Target:       {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}")
    print(f"  Account:      {SNOWFLAKE_ACCOUNT}")
    print(f"  Readings:     {NUM_READINGS}")
    print(f"  Interval:     {STREAM_INTERVAL}s")
    print("=" * 60)

    client, channel = create_streaming_client()
    try:
        run_simulation(channel)
    except KeyboardInterrupt:
        print("\nInterrupted. Flushing remaining data...")
        channel.close(wait_for_flush=True, timeout_seconds=30)
    finally:
        if not client.is_closed():
            client.close()
        print("Client closed.")

    print(f"\nVerify with:\n  SELECT SENSOR_DATA_JSON FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE} "
          f"WHERE SENSOR_DATA_JSON:device_id = '{DEVICE_ID}' ORDER BY SENSOR_DATA_JSON:timestamp DESC LIMIT 20;")


if __name__ == "__main__":
    main()
