#!/usr/bin/env python3
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

from snowflake.ingest.streaming import StreamingIngestClient

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
SNOWFLAKE_PIPE = f"{SNOWFLAKE_TABLE}-STREAMING"

DEVICE_ID = os.getenv("DEVICE_ID", "test-turbine-sim")
CHANNEL_NAME = f"{DEVICE_ID}-channel"
STREAM_INTERVAL = float(os.getenv("STREAM_INTERVAL", str(_dev["stream_interval"])))
NUM_READINGS = int(os.getenv("NUM_READINGS", "500"))


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

    def tick(self):
        self.time_offset += STREAM_INTERVAL
        self.pot_angle = 128 + 127 * math.sin(self.time_offset * 0.3)
        self.light_level = int(128 + 100 * math.sin(self.time_offset * 0.05 + 1.0))
        self.light_level = max(0, min(255, self.light_level + random.randint(-5, 5)))

    def read_motor_adc(self):
        return max(0, min(255, int(self.pot_angle + random.gauss(0, 2))))

    def read_photoresistor_adc(self):
        return self.light_level

    def get_motor_state(self, adc_value):
        value = adc_value - 128
        if value > 0:
            direction = "forward"
        elif value < 0:
            direction = "backward"
        else:
            direction = "stopped"
        duty_cycle = abs(value) * 100 / 127
        return direction, duty_cycle

    def get_system_metrics(self):
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


def create_streaming_client():
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


def build_row(device_id, motor_adc, direction, duty_cycle, light_adc, light_voltage, system_metrics):
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
    return {"sensor_data_json": payload}


def run_simulation(channel):
    sim = SimulatedSensors()
    print(f"\nStreaming {NUM_READINGS} simulated readings to Snowflake...\n")

    for seq in range(NUM_READINGS):
        sim.tick()
        motor_adc = sim.read_motor_adc()
        direction, duty_cycle = sim.get_motor_state(motor_adc)
        light_adc = sim.read_photoresistor_adc()
        light_voltage = light_adc / 255.0 * 3.3
        system_metrics = sim.get_system_metrics()

        row = build_row(DEVICE_ID, motor_adc, direction, duty_cycle, light_adc, light_voltage, system_metrics)
        channel.append_row(row, offset_token=str(seq))

        if seq % 10 == 0 or seq == NUM_READINGS - 1:
            print(f"  [{seq + 1}/{NUM_READINGS}] Motor: {motor_adc} ({direction}, {duty_cycle:.0f}%) | "
                  f"Light: {light_adc} ({light_voltage:.2f}V) | "
                  f"CPU: {system_metrics['cpu_temperature_celsius']}°C")

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
