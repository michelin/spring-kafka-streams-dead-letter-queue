<div align="center">

Work in progress...

<img src=".readme/logo.png" alt="Apache Kafka"/>

# Kafka Streams Dead Letter Queue in Spring Kafka

[![GitHub Build](https://img.shields.io/github/actions/workflow/status/michelin/spring-kafka-streams-dead-letter-queue/build.yml?branch=main&logo=github&style=for-the-badge)](https://github.com/michelin/spring-kafka-streams-dead-letter-queue/actions/workflows/build.yml)
[![Spring Boot Version](https://img.shields.io/badge/dynamic/xml?url=https%3A%2F%2Fraw.githubusercontent.com%2Fmichelin%2Fspring-kafka-streams-dead-letter-queue%2Fmain%2Fpom.xml&query=%2F*%5Blocal-name()%3D'project'%5D%2F*%5Blocal-name()%3D'parent'%5D%2F*%5Blocal-name()%3D'version'%5D%2Ftext()&style=for-the-badge&logo=spring-boot&label=version)](https://github.com/michelin/spring-kafka-streams-dead-letter-queue/blob/main/pom.xml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache&style=for-the-badge)](https://opensource.org/licenses/Apache-2.0)

[Prerequisites](#prerequisites) • [Examples](#examples) • [Michelin IT Blog](#michelin-it-blog)

Code sample for Spring-friendly Kafka Streams Dead Letter Queue ([KIP-1034](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1034%3A+Dead+letter+queue+in+Kafka+Streams)) in Spring Kafka.

</div>

## Prerequisites

- Java 25
- Maven
- Docker

## Examples

| Module                                                                | Description                                                   | Class                                                                                                                            |
|:----------------------------------------------------------------------|---------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| [Dead Letter Destination Resolver](/dead-letter-destination-resolver) | Spring-friendly dead letter queue routing logic               | `KafkaStreamsDeadLetterDestinationResolver`, `KafkaStreamsConfiguration`                                                         |
| [Dead Letter Publishing Recoverer](/dead-letter-publishing-recoverer) | Original Spring Kafka dead letter queue                       | `DeadLetterPublishingRecoverer`, `KafkaStreamsConfiguration`                                                                     |
| [Dead Letter Topic Name](/dead-letter-topic-name)                     | Straightforward way to send to a predefined dead letter topic | `StreamsBuilderFactoryBean`, `errors.dead.letter.queue.topic.name`                                                               |
| [Recovering Exception Handlers](/recovering-exception-handlers)       | Use the provided recovering exception handlers                | `KafkaStreamsConfiguration`, `deserialization.exception.handler`, `processing.exception.handler`, `production.exception.handler` |

## Michelin IT Blog

_Soon to be published._
