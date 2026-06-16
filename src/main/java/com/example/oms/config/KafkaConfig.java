package com.example.oms.config;

import com.example.oms.domain.Topics;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.support.converter.ByteArrayJsonMessageConverter;
import org.springframework.kafka.support.converter.RecordMessageConverter;

/**
 * Topic declarations + the message converter that lets @KafkaListener methods
 * receive strongly-typed payloads (the converter deserializes JSON to the
 * method's parameter type; polymorphic OrderEvent is resolved via @JsonTypeInfo).
 *
 * Local dev uses 6 partitions / replicas=1. Production: partitions=30,
 * replicas=3, and min.insync.replicas=2 (set at broker or per-topic).
 */
@Configuration
public class KafkaConfig {

    @Bean
    RecordMessageConverter jsonMessageConverter() {
        return new ByteArrayJsonMessageConverter();
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic orderEvents() {
        return TopicBuilder.name(Topics.ORDER_EVENTS).partitions(6).replicas(1).build();
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic orderState() {
        return TopicBuilder.name(Topics.ORDER_STATE).partitions(6).replicas(1)
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                .build();
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic inventoryCommands() {
        return TopicBuilder.name(Topics.INVENTORY_COMMANDS).partitions(6).replicas(1).build();
    }

    @Bean
    org.apache.kafka.clients.admin.NewTopic paymentCommands() {
        return TopicBuilder.name(Topics.PAYMENT_COMMANDS).partitions(6).replicas(1).build();
    }
}
