# Kafka -> MongoDB Streaming Setup

MongoDB URI:
`mongodb://localhost:27017/`

MongoDB database:
`energy_streaming`

Collections:
- smart_meters
- scada
- grid_sensors
- weather
- ev_chargers
- battery_storage
- trading

## 1. Install MongoDB Python driver

```bash
pip install pymongo
```

Or:

```bash
pip install -r requirements_mongodb.txt
```

## 2. Start MongoDB

Make sure your local MongoDB server is running on port 27017.

## 3. Start Kafka

From your existing Kafka/Docker project:

```bash
docker compose up -d
```

## 4. Start the MongoDB consumer FIRST

From the project root:

```bash
python consumers/all_topics_consumer.py
```

If your consumer file is in the project root instead, run:

```bash
python all_topics_consumer.py
```

You should see:

`Kafka -> MongoDB streaming consumer started`

## 5. Start all seven producers

In another terminal:

```bash
python producers/run_all_producers.py
```

The existing producer files do not need MongoDB code because they already publish the generated JSON records to Kafka. The MongoDB consumer is the bridge from Kafka to MongoDB.

## 6. View the data in MongoDB Compass

Connect using:

`mongodb://localhost:27017/`

Open:

`energy_streaming`

You should see the seven collections listed above.

New documents will appear continuously as Kafka messages arrive.

## Important

Start the MongoDB consumer before the producers for the cleanest demo.

The consumer uses `auto_offset_reset="earliest"` for a new consumer group, so messages already present in Kafka can also be consumed.

Each document keeps the original producer fields and adds:
- `_id` = event_id
- `_kafka_topic`
- `_kafka_partition`
- `_kafka_offset`
- `_received_at`
