package com.rajasekhar.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration with exactly-once semantics and DLQ error handling.
 *
 * Key design decisions:
 *
 * PRODUCER:
 *  - enable.idempotence=true: Kafka deduplicates retries at broker level.
 *    Without this, a network blip causing a retry → duplicate message.
 *  - transactional.id: Enables atomic writes across multiple topics in one
 *    transaction. Critical for wallet debits — either the event is published
 *    AND the DB is updated, or neither happens.
 *  - acks=all: Wait for all in-sync replicas to acknowledge before returning.
 *    Slowest but safest — no data loss even if leader broker dies.
 *
 * CONSUMER:
 *  - isolation.level=read_committed: Consumer only reads messages from
 *    COMMITTED transactions. Filters out in-flight or aborted messages.
 *    Without this, consumer might process a message that gets rolled back.
 *  - AckMode=MANUAL_IMMEDIATE: We control exactly when offset is committed.
 *    Default auto-commit can commit offset before processing succeeds → data loss.
 *
 * ERROR HANDLING:
 *  - FixedBackOff(1000ms, 3 retries): On failure, retry 3 times with 1s gap.
 *  - After 3 failures: DeadLetterPublishingRecoverer routes to {topic}.DLQ
 *  - This prevents a single bad message from blocking the entire partition.
 */
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // ── Topic definitions ─────────────────────────────────────────────────

    @Bean
    public KafkaAdmin.NewTopics kafkaTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(KafkaTopics.ORDERS_CREATED).partitions(3).replicas(1).build(),
                TopicBuilder.name(KafkaTopics.ORDERS_CREATED_DLQ).partitions(1).replicas(1).build(),
                TopicBuilder.name(KafkaTopics.FRAUD_ALERTS).partitions(3).replicas(1).build(),
                TopicBuilder.name(KafkaTopics.FRAUD_ALERTS_DLQ).partitions(1).replicas(1).build(),
                TopicBuilder.name(KafkaTopics.WALLET_TRANSACTIONS).partitions(3).replicas(1).build(),
                TopicBuilder.name(KafkaTopics.WALLET_TRANSACTIONS_DLQ).partitions(1).replicas(1).build()
        );
    }

    // ── Producer ──────────────────────────────────────────────────────────

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // Exactly-once producer settings
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // Transactional producer — enables atomic multi-topic writes
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kafka-streaming-tx-");

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ── Consumer ──────────────────────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.rajasekhar.kafka.domain.event");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Object.class);

        // Exactly-once consumer: only read committed transactions
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // Manual offset commit — we commit only after successful processing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ── Listener container factory with DLQ error handler ─────────────────

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // Manual ack mode — offset committed only after successful processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // DLQ error handler: retry 3 times with 1s gap, then route to DLQ topic
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
            new DeadLetterPublishingRecoverer(kafkaTemplate()),
            new FixedBackOff(1000L, 3L)
        );
        factory.setCommonErrorHandler(errorHandler);

        // Concurrency = number of partitions for parallel processing
        factory.setConcurrency(3);

        return factory;
    }
}
