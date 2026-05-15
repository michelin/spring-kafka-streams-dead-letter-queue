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

import java.util.Properties;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.streams.RecoveringProcessingExceptionHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

class KafkaStreamsAppTest {
    private final KafkaStreamsApp kafkaStreamsApp = new KafkaStreamsApp();

    @Test
    void shouldFilterDeliveriesAndSendInvalidToDlq() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        kafkaStreamsApp.topology(streamsBuilder);

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.put(
                StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, RecoveringProcessingExceptionHandler.class);
        properties.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "default-dlq-topic");

        try (TopologyTestDriver driver = new TopologyTestDriver(streamsBuilder.build(), properties)) {
            TestInputTopic<String, DeliveryBooked> inputTopic = driver.createInputTopic(
                    "delivery-booked-topic", new StringSerializer(), new JacksonJsonSerializer<>());

            TestOutputTopic<String, DeliveryBooked> outputTopic = driver.createOutputTopic(
                    "filtered-delivery-booked-topic",
                    new StringDeserializer(),
                    new JacksonJsonDeserializer<>(DeliveryBooked.class));

            TestOutputTopic<String, DeliveryBooked> dlqTopic = driver.createOutputTopic(
                    "default-dlq-topic", new StringDeserializer(), new JacksonJsonDeserializer<>(DeliveryBooked.class));

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
