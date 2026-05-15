/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.errors.ErrorHandlerContext;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.streams.KafkaStreamsDeadLetterDestinationResolver;
import org.springframework.kafka.streams.RecoveringProcessingExceptionHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

@ExtendWith(MockitoExtension.class)
class KafkaStreamsAppTest {
    @Mock
    private ErrorHandlerContext context;

    private final KafkaStreamsApp kafkaStreamsApp = new KafkaStreamsApp();
    private final KafkaStreamsDeadLetterDestinationResolver resolver = kafkaStreamsApp.resolver();

    @Test
    void shouldRouteToCorrectDlq() {
        ConsumerRecord<String, DeliveryBooked> nullTiresRecord = new ConsumerRecord<>(
                "delivery-booked-topic", 0, 0L, "DEL001", new DeliveryBooked("DEL001", "TRK001", null, "Bordeaux"));
        TopicPartition nullTiresResult = resolver.resolve(context, nullTiresRecord, new RuntimeException());
        assertEquals("null-number-of-tires-dlq-topic", nullTiresResult.topic());
        assertEquals(-1, nullTiresResult.partition());

        ConsumerRecord<String, DeliveryBooked> validRecord = new ConsumerRecord<>(
                "delivery-booked-topic", 0, 1L, "DEL002", new DeliveryBooked("DEL002", "TRK002", 7, "Paris"));
        TopicPartition invalidDeliveryResult = resolver.resolve(
                context, validRecord, new InvalidDeliveryException("Number of tires cannot be negative"));
        assertEquals("invalid-delivery-dlq-topic", invalidDeliveryResult.topic());
        assertEquals(-1, invalidDeliveryResult.partition());

        when(context.processorNodeId()).thenReturn("select-key-processor");
        TopicPartition selectKeyResult = resolver.resolve(context, validRecord, new RuntimeException());
        assertEquals("select-key-processor-dlq-topic", selectKeyResult.topic());
        assertEquals(-1, selectKeyResult.partition());

        when(context.processorNodeId()).thenReturn("filter-tires-processor");
        TopicPartition defaultResult = resolver.resolve(context, validRecord, new RuntimeException());
        assertEquals("default-dlq-topic", defaultResult.topic());
        assertEquals(0, defaultResult.partition());
    }

    @Test
    void shouldFilterDeliveriesAndSendInvalidToDlq() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        kafkaStreamsApp.topology(streamsBuilder);

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(
                StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, RecoveringProcessingExceptionHandler.class);
        properties.put(RecoveringProcessingExceptionHandler.DLQ_DESTINATION_RESOLVER, resolver);

        try (TopologyTestDriver driver = new TopologyTestDriver(streamsBuilder.build(), properties)) {
            TestInputTopic<String, DeliveryBooked> inputTopic = driver.createInputTopic(
                    "delivery-booked-topic", new StringSerializer(), new JacksonJsonSerializer<>());

            TestOutputTopic<String, DeliveryBooked> outputTopic = driver.createOutputTopic(
                    "filtered-delivery-booked-topic",
                    new StringDeserializer(),
                    new JacksonJsonDeserializer<>(DeliveryBooked.class));

            TestOutputTopic<String, DeliveryBooked> dlqTopic = driver.createOutputTopic(
                    "invalid-delivery-dlq-topic",
                    new StringDeserializer(),
                    new JacksonJsonDeserializer<>(DeliveryBooked.class));

            inputTopic.pipeInput("DEL001", new DeliveryBooked("DEL001", "TRK001", 18, "Bordeaux"));
            inputTopic.pipeInput("DEL002", new DeliveryBooked("DEL002", "TRK002", -3, "Paris"));
            inputTopic.pipeInput("DEL003", new DeliveryBooked("DEL003", "TRK003", 10, "Lyon"));
            inputTopic.pipeInput("DEL004", new DeliveryBooked("DEL004", "TRK004", 5, "Clermont-Ferrand"));

            assertEquals(2, outputTopic.getQueueSize());
            TestRecord<String, DeliveryBooked> first = outputTopic.readRecord();
            assertEquals("DEL001TRK001", first.key());

            TestRecord<String, DeliveryBooked> second = outputTopic.readRecord();
            assertEquals("DEL003TRK003", second.key());
            assertTrue(outputTopic.isEmpty());

            assertEquals(1, dlqTopic.getQueueSize());
            TestRecord<String, DeliveryBooked> dlqRecord = dlqTopic.readRecord();
            assertEquals("DEL002", dlqRecord.key());
        }
    }
}
