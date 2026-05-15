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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import org.springframework.kafka.streams.RecoveringProcessingExceptionHandler;
import org.springframework.kafka.streams.RecoveringProductionExceptionHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

class KafkaStreamsAppTest {
    private final KafkaStreamsApp kafkaStreamsApp = new KafkaStreamsApp();

    @Test
    void shouldFilterDeliveriesWithAtLeast10Tires() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        kafkaStreamsApp.topology(streamsBuilder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        try (TopologyTestDriver driver = new TopologyTestDriver(streamsBuilder.build(), props)) {
            TestInputTopic<String, DeliveryBooked> inputTopic = driver.createInputTopic(
                    "delivery-booked-topic", new StringSerializer(), new JacksonJsonSerializer<>());

            TestOutputTopic<String, DeliveryBooked> outputTopic = driver.createOutputTopic(
                    "filtered-delivery-booked-topic",
                    new StringDeserializer(),
                    new JacksonJsonDeserializer<>(DeliveryBooked.class));

            inputTopic.pipeInput("DEL001", new DeliveryBooked("DEL001", "TRK001", 18, "Bordeaux"));
            inputTopic.pipeInput("DEL002", new DeliveryBooked("DEL002", "TRK002", 7, "Paris"));
            inputTopic.pipeInput("DEL003", new DeliveryBooked("DEL003", "TRK003", 10, "Lyon"));
            inputTopic.pipeInput("DEL004", new DeliveryBooked("DEL004", "TRK004", 5, "Clermont-Ferrand"));

            assertEquals(2, outputTopic.getQueueSize());

            TestRecord<String, DeliveryBooked> first = outputTopic.readRecord();
            assertEquals("DEL001TRK001", first.key());
            assertEquals(18, first.value().numberOfTires());

            TestRecord<String, DeliveryBooked> second = outputTopic.readRecord();
            assertEquals("DEL003TRK003", second.key());
            assertEquals(10, second.value().numberOfTires());

            assertTrue(outputTopic.isEmpty());
        }
    }

    @Test
    void shouldConfigureKafkaStreamsWithRecoverer() {
        KafkaProperties kafkaProperties = new KafkaProperties();
        ProducerFactory<byte[], byte[]> producerFactory = kafkaStreamsApp.producerFactory(kafkaProperties);
        KafkaTemplate<byte[], byte[]> kafkaTemplate = kafkaStreamsApp.kafkaTemplate(producerFactory);
        DeadLetterPublishingRecoverer recoverer = kafkaStreamsApp.recoverer(kafkaTemplate);
        KafkaStreamsConfiguration config = kafkaStreamsApp.configs(kafkaProperties, recoverer);
        Properties props = config.asProperties();

        assertEquals(recoverer, props.get(RecoveringDeserializationExceptionHandler.RECOVERER));
        assertEquals(recoverer, props.get(RecoveringProcessingExceptionHandler.RECOVERER));
        assertEquals(recoverer, props.get(RecoveringProductionExceptionHandler.RECOVERER));
    }
}
