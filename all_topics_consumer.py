"""
Kafka consumer -> MySQL streaming consumer.

Consumes all 7 Kafka topics and writes each incoming record
to the corresponding MySQL table.

Pipeline:
Kafka -> MySQL -> Grafana
"""

import json
import sys
import os
from datetime import datetime, timezone

import mysql.connector
from mysql.connector import Error, IntegrityError
from kafka import KafkaConsumer

sys.path.append(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

from config import BOOTSTRAP_SERVERS, ALL_TOPICS


# =========================================================
# MySQL Configuration
# =========================================================

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "Energy@12345"),
    "database": os.getenv("MYSQL_DATABASE", "energy_streaming"),
}


# =========================================================
# Kafka Topic -> MySQL Table
# =========================================================

TOPIC_TO_TABLE = {
    "smart-meter-readings": "smart_meter_readings",
    "scada-plant-monitoring": "scada_plant_monitoring",
    "grid-sensor-pmu": "grid_sensor_pmu",
    "weather-feed": "weather_feed",
    "ev-charging-events": "ev_charging_events",
    "battery-storage-status": "battery_storage_status",
    "energy-trading-prices": "energy_trading_prices",
}


def get_table_name(topic):
    """Map Kafka topic to MySQL table."""

    if topic in TOPIC_TO_TABLE:
        return TOPIC_TO_TABLE[topic]

    normalized = topic.replace("-", "_").split(".")[-1]

    for known_topic in TOPIC_TO_TABLE:
        if normalized == known_topic:
            return TOPIC_TO_TABLE[known_topic]

    return None


def parse_timestamp(value):
    """Convert Kafka ISO timestamp into MySQL DATETIME."""

    if not value:
        return None

    if isinstance(value, datetime):
        return value.replace(tzinfo=None)

    value = str(value).strip()

    try:
        dt = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )

        if dt.tzinfo is not None:
            dt = dt.astimezone(
                timezone.utc
            ).replace(tzinfo=None)

        return dt

    except ValueError:
        print(
            f"[WARNING] Could not parse timestamp: {value}"
        )
        return None


# =========================================================
# Insert Kafka Record into MySQL
# =========================================================

def insert_record(cursor, topic, record):

    table = get_table_name(topic)

    if table is None:
        raise ValueError(
            f"No MySQL table mapping found for topic: {topic}"
        )

    event_id = record.get("event_id")
    timestamp = parse_timestamp(
        record.get("timestamp")
    )

    # -----------------------------------------------------
    # SMART METERS
    # -----------------------------------------------------

    if table == "smart_meter_readings":

        sql = """
            INSERT INTO smart_meter_readings
            (
                event_id,
                meter_id,
                region,
                timestamp,
                consumption_kwh,
                voltage,
                customer_type
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            event_id,
            record.get("meter_id"),
            record.get("region"),
            timestamp,
            record.get("consumption_kwh"),
            record.get("voltage"),
            record.get("customer_type"),
        )

    # -----------------------------------------------------
    # SCADA
    # -----------------------------------------------------

    elif table == "scada_plant_monitoring":

        sql = """
            INSERT INTO scada_plant_monitoring
            (
                event_id,
                plant_id,
                plant_type,
                timestamp,
                power_output_mw,
                turbine_speed_rpm,
                panel_voltage
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        values = (
            event_id,
            record.get("plant_id"),
            record.get("plant_type"),
            timestamp,
            record.get("power_output_mw"),
            record.get("turbine_speed_rpm"),
            record.get("panel_voltage"),
        )

    # -----------------------------------------------------
    # GRID SENSORS
    # -----------------------------------------------------

    elif table == "grid_sensor_pmu":

        sql = """
            INSERT INTO grid_sensor_pmu
            (
                event_id,
                sensor_id,
                timestamp,
                voltage_kv,
                frequency_hz,
                load_mw
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        values = (
            event_id,
            record.get("sensor_id"),
            timestamp,
            record.get("voltage_kv"),
            record.get("frequency_hz"),
            record.get("load_mw"),
        )

    # -----------------------------------------------------
    # WEATHER
    # -----------------------------------------------------

    elif table == "weather_feed":

        sql = """
            INSERT INTO weather_feed
            (
                event_id,
                location,
                timestamp,
                wind_speed_mps,
                solar_irradiance_wm2,
                temperature_c
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        values = (
            event_id,
            record.get("location"),
            timestamp,
            record.get("wind_speed_mps"),
            record.get("solar_irradiance_wm2"),
            record.get("temperature_c"),
        )

    # -----------------------------------------------------
    # EV CHARGERS
    # -----------------------------------------------------

    elif table == "ev_charging_events":

        sql = """
            INSERT INTO ev_charging_events
            (
                event_id,
                station_id,
                timestamp,
                event_type,
                power_draw_kw
            )
            VALUES (%s, %s, %s, %s, %s)
        """

        values = (
            event_id,
            record.get("station_id"),
            timestamp,
            record.get("event_type"),
            record.get("power_draw_kw"),
        )

    # -----------------------------------------------------
    # BATTERY STORAGE
    # -----------------------------------------------------

    elif table == "battery_storage_status":

        sql = """
            INSERT INTO battery_storage_status
            (
                event_id,
                battery_id,
                timestamp,
                state,
                state_of_charge_pct
            )
            VALUES (%s, %s, %s, %s, %s)
        """

        values = (
            event_id,
            record.get("battery_id"),
            timestamp,
            record.get("state"),
            record.get("state_of_charge_pct"),
        )

    # -----------------------------------------------------
    # ENERGY TRADING
    # -----------------------------------------------------

    elif table == "energy_trading_prices":

        sql = """
            INSERT INTO energy_trading_prices
            (
                event_id,
                market,
                timestamp,
                price_per_mwh,
                supply_mw,
                demand_mw
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        values = (
            event_id,
            record.get("market"),
            timestamp,
            record.get("price_per_mwh"),
            record.get("supply_mw"),
            record.get("demand_mw"),
        )

    else:
        raise ValueError(
            f"Unsupported Kafka topic: {topic}"
        )

    cursor.execute(sql, values)

    return table


# =========================================================
# Main Consumer
# =========================================================

def run():

    # -----------------------------------------------------
    # Connect to MySQL
    # -----------------------------------------------------

    try:

        db = mysql.connector.connect(
            **MYSQL_CONFIG
        )

        if not db.is_connected():
            raise RuntimeError(
                "MySQL connection failed."
            )

        print("=" * 70)
        print("Kafka -> MySQL streaming consumer started")
        print(
            f"MySQL host : "
            f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}"
        )
        print(
            f"MySQL DB   : "
            f"{MYSQL_CONFIG['database']}"
        )
        print(
            f"Kafka topics: {ALL_TOPICS}"
        )
        print("=" * 70)

    except Error as e:

        print(
            f"[MYSQL ERROR] "
            f"Could not connect to MySQL: {e}"
        )

        return

    cursor = db.cursor()

    # -----------------------------------------------------
    # Connect to Kafka
    # -----------------------------------------------------

    consumer = KafkaConsumer(

        *ALL_TOPICS,

        bootstrap_servers=BOOTSTRAP_SERVERS,

        auto_offset_reset="earliest",

        enable_auto_commit=False,

        value_deserializer=lambda v:
            json.loads(
                v.decode("utf-8")
            ),

        group_id="mysql-grafana-consumer-group",
    )

    # -----------------------------------------------------
    # Process Messages
    # -----------------------------------------------------

    try:

        for msg in consumer:

            try:

                record = msg.value

                if not isinstance(
                    record,
                    dict
                ):

                    print(
                        f"[SKIPPED] "
                        f"Non-dictionary message "
                        f"from {msg.topic}: "
                        f"{record}"
                    )

                    continue

                table = insert_record(
                    cursor,
                    msg.topic,
                    record
                )

                # First commit to MySQL
                db.commit()

                # Then commit Kafka offset
                consumer.commit()

                print(
                    f"[MySQL] inserted -> "
                    f"{table} | "
                    f"event_id="
                    f"{record.get('event_id')} | "
                    f"topic={msg.topic}"
                )

            except IntegrityError as e:

                # Duplicate event_id
                db.rollback()

                # Message was already stored
                consumer.commit()

                print(
                    f"[MySQL] duplicate/already "
                    f"processed -> "
                    f"event_id="
                    f"{record.get('event_id')}"
                )

            except Exception as message_error:

                db.rollback()

                print(
                    f"[ERROR] Could not process "
                    f"message from topic "
                    f"'{msg.topic}': "
                    f"{message_error}"
                )

    except KeyboardInterrupt:

        print(
            "\nConsumer stopped by user."
        )

    finally:

        consumer.close()
        cursor.close()
        db.close()

        print(
            "Kafka consumer and MySQL "
            "connection closed."
        )


# =========================================================
# Start
# =========================================================

if __name__ == "__main__":
    run()