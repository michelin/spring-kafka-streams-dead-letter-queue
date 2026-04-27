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

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import org.springframework.kafka.streams.RecoveringProcessingExceptionHandler;
import org.springframework.kafka.streams.RecoveringProductionExceptionHandler;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.springframework.stereotype.Service;

@Service
public class KafkaStreamsApp {

    /**
     * Sets recovering exception handlers programmatically as an alternative to {@code application.yml}:
     *
     * <pre>{@code
     * spring:
     *   kafka:
     *     streams:
     *       properties:
     *         deserialization.exception.handler:
     *           "org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler"
     *         processing.exception.handler:
     *           "org.springframework.kafka.streams.RecoveringProcessingExceptionHandler"
     *         production.exception.handler:
     *           "org.springframework.kafka.streams.RecoveringProductionExceptionHandler"
     * }</pre>
     *
     * @param kafkaProperties Kafka properties.
     * @return Kafka Streams configuration.
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration configs(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildStreamsProperties());
        props.put(
                StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                RecoveringDeserializationExceptionHandler.class);
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, RecoveringProcessingExceptionHandler.class);
        props.put(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, RecoveringProductionExceptionHandler.class);
        return new KafkaStreamsConfiguration(props);
    }

    /**
     * Defines the Kafka Streams topology.
     *
     * @param streamsBuilder streams builder.
     * @return source stream.
     */
    @Bean
    public KStream<?, ?> topology(StreamsBuilder streamsBuilder) {
        JacksonJsonSerde<DeliveryBooked> serde = new JacksonJsonSerde<>(DeliveryBooked.class);

        KStream<String, DeliveryBooked> stream =
                streamsBuilder.stream("delivery-booked-topic", Consumed.with(Serdes.String(), serde));

        stream.selectKey((_, value) -> value.deliveryId().concat(value.truckId()), Named.as("select-key-processor"))
                .filter(
                        (_, value) -> {
                            if (value.numberOfTires() < 0) {
                                throw new InvalidDeliveryException("Number of tires cannot be negative");
                            }

                            return value.numberOfTires() >= 10;
                        },
                        Named.as("filter-tires-processor"))
                .to("filtered-delivery-booked-topic", Produced.with(Serdes.String(), serde));

        return stream;
    }
}
