# Recovery Exception Handlers

This module demonstrates how to programmatically configure the recovery exception handlers introduced in Spring Kafka 4.1, 
with no routing to a dead letter topic, but instead stopping the stream in case of errors.

## Prerequisites

To compile and run this demo, you’ll need:

- Java 25
- Maven
- Docker

## Running the Application

To run the application manually:

- Start a [Confluent Platform](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) in a Docker environment.
- Create a topic named `delivery-booked-topic`.
- Start the Kafka Streams application.

To run the application in Docker, use the following command:

```bash
docker-compose up -d
```

This will start the following services in Docker:

- Kafka Broker
- Control Center
- Kafka Streams Recovery Exception Handlers

## Try It Out

Using the [Kafkagen](https://github.com/michelin/kafkagen) `produce` command, you can produce `DeliveryBooked` events to the `delivery-booked-topic` topic.

```bash
kafkagen produce -f ../.kafkagen/default-record.json
```

To trigger the recovery processing exception handler, produce a record with a missing `numberOfTires` field. 
This will result in a `NullPointerException` and stop the stream:

```bash
kafkagen produce -f ../.kafkagen/no-tires-record.json
```