# Dead Letter Destination Resolver

This module demonstrates how to use the `KafkaStreamsDeadLetterDestinationResolver` to define the destination for records that fail processing.

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
- Kafka Streams Dead Letter Destination Resolver

## Try It Out

Using the [Kafkagen](https://github.com/michelin/kafkagen) `produce` command, you can produce `DeliveryBooked` events to the `delivery-booked-topic` topic.

```bash
kafkagen produce -f ../.kafkagen/default-record.json
```

To trigger dead letter queue routing from the processing exception handler, produce records:

- With a missing `numberOfTires` field to route to `null-number-of-tires-dlq-topic`:

```bash
kafkagen produce -f ../.kafkagen/no-tires-record.json
```

- With a negative `numberOfTires` to route to `invalid-delivery-dlq-topic`:

```bash
kafkagen produce -f ../.kafkagen/invalid-delivery-record.json
```

- With a missing `deliveryId` field to route to `select-key-processor-dlq-topic`:

```bash
kafkagen produce -f ../.kafkagen/no-delivery-id-record.json
```

Alternatively, to trigger routing from the deserialization exception handler, produce a record with an invalid value type to route to `default-dlq-topic`:

```bash
kafkagen produce -f ../.kafkagen/wrong-type-record.json
```