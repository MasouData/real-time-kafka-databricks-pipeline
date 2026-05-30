# Kafka Producer

This folder contains a simple Python Kafka producer used to generate sample streaming events for the main Databricks pipeline.

The producer sends JSON messages to a Confluent Kafka topic.

## File

```text
sample_producer.py
```

## Purpose

The producer simulates an external application that continuously sends events to Kafka.

In this project, the producer sends sample messages with the following structure:

```json
{
  "id": 1,
  "message": "sample message 1"
}
```

These messages are later consumed by Databricks Structured Streaming and processed through the Bronze, Silver, Silver Enriched, and Gold layers.

## Kafka Topic

The producer sends messages to:

```text
my_first_topic
```

## Required Environment Variables

The producer reads the Confluent Kafka credentials from environment variables:

```text
KAFKA_API_KEY
KAFKA_API_SECRET
```

This avoids hardcoding secrets in the source code.

## Install Dependency

From the `producer` folder, install the Confluent Kafka Python client:

```bash
pip install confluent-kafka
```

## Set Environment Variables

### Windows PowerShell

```powershell
$env:KAFKA_API_KEY="your_confluent_api_key"
$env:KAFKA_API_SECRET="your_confluent_api_secret"
```

### macOS / Linux

```bash
export KAFKA_API_KEY="your_confluent_api_key"
export KAFKA_API_SECRET="your_confluent_api_secret"
```

## Run the Producer

```bash
python sample_producer.py
```

The script produces 10 sample messages.

Example output:

```text
Producing message : key=key-0 and value={"id": 0, "message": "sample message 0"}
Producing message : key=key-1 and value={"id": 1, "message": "sample message 1"}
...
```

## How It Works

The producer configuration contains:

```python
config = {
    "bootstrap.servers": "...",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.getenv("KAFKA_API_KEY"),
    "sasl.password": os.getenv("KAFKA_API_SECRET"),
    "client.id": "transaction-producer"
}
```

The important parts are:

* `bootstrap.servers`: Confluent Kafka cluster endpoint
* `security.protocol`: encrypted connection to Kafka
* `sasl.mechanisms`: authentication mechanism
* `sasl.username`: Confluent API key
* `sasl.password`: Confluent API secret
* `client.id`: name of this producer client

Each message has:

* `key`: Kafka message key, for example `key-0`
* `value`: JSON event payload, for example `{"id": 0, "message": "sample message 0"}`

## Notes

This producer is intentionally simple because the goal is to test the end-to-end streaming pipeline.

Possible future improvements:

* Add a delivery callback to confirm successful message delivery
* Add error handling
* Generate more realistic event data
* Send messages continuously instead of only 10 messages
* Add Docker support for running the producer in a container
* Add Schema Registry for stronger message schema management

