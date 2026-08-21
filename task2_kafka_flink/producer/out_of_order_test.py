import json
import time

from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = "localhost:9192"
KAFKA_TOPIC = "traffic-telemetry"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


events = [
    {
        "record_id": "test-001",
        "atd_device_id": 9999,
        "read_date": "2017-06-01T01:00:00.000",
        "intersection_name": "TEST INTERSECTION",
        "direction": "NORTHBOUND",
        "movement": "THRU",
        "heavy_vehicle": False,
        "volume": 10,
        "speed_average": 30.0,
        "bin_duration": 900,
    },
    {
        "record_id": "test-002",
        "atd_device_id": 9999,
        "read_date": "2017-06-01T01:00:08.000",
        "intersection_name": "TEST INTERSECTION",
        "direction": "NORTHBOUND",
        "movement": "THRU",
        "heavy_vehicle": False,
        "volume": 20,
        "speed_average": 31.0,
        "bin_duration": 900,
    },
    {
        # Intentionally arrives late, but only 5 seconds behind
        "record_id": "test-003",
        "atd_device_id": 9999,
        "read_date": "2017-06-01T01:00:03.000",
        "intersection_name": "TEST INTERSECTION",
        "direction": "NORTHBOUND",
        "movement": "THRU",
        "heavy_vehicle": False,
        "volume": 30,
        "speed_average": 29.0,
        "bin_duration": 900,
    },
]


for event in events:
    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()

    print(
        f"Sent: {event['record_id']} "
        f"time={event['read_date']} "
        f"volume={event['volume']}"
    )

    time.sleep(1)

advance_event = [
    {
        "record_id": "test-004",
        "atd_device_id": 9999,
        "read_date": "2017-06-01T01:15:20.000",
        "intersection_name": "TEST INTERSECTION",
        "direction": "NORTHBOUND",
        "movement": "THRU",
        "heavy_vehicle": False,
        "volume": 1,
        "speed_average": 30.0,
        "bin_duration": 900,
    },
    {
        "record_id": "test-005",
        "atd_device_id": 9999,
        "read_date": "2017-06-01T01:15:22.000",
        "intersection_name": "TEST INTERSECTION",
        "direction": "NORTHBOUND",
        "movement": "THRU",
        "heavy_vehicle": False,
        "volume": 2,
        "speed_average": 32.0,
        "bin_duration": 900,
    },
    {
        "record_id": "test-006",
        "atd_device_id": 9999,
        "read_date": "2017-06-01T01:30:35.000",
        "intersection_name": "TEST INTERSECTION",
        "direction": "NORTHBOUND",
        "movement": "THRU",
        "heavy_vehicle": False,
        "volume": 3,
        "speed_average": 35.0,
        "bin_duration": 900,
    }
]

for event in advance_event:
    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()

    print(
        f"Sent: {event['record_id']} "
        f"time={event['read_date']} "
        f"volume={event['volume']}"
    )

    time.sleep(1)

producer.close()