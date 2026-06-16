package com.example.oms.config;

import com.example.oms.service.EventLogStore;
import com.example.oms.web.EventEntry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.ProducerListener;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Configuration
public class KafkaEventCapture {

    @Autowired
    void configure(
            KafkaTemplate<String, Object> template,
            ConcurrentKafkaListenerContainerFactory<String, byte[]> factory,
            EventLogStore store) {

        template.setProducerListener(new ProducerListener<>() {
            @Override
            public void onSuccess(ProducerRecord<String, Object> rec, RecordMetadata meta) {
                String type = rec.value() == null ? "null" : rec.value().getClass().getSimpleName();
                String payload = rec.value() == null ? "" : rec.value().toString();
                store.addProduced(new EventEntry(rec.topic(), rec.key(), type, payload, Instant.now()));
            }
        });

        factory.setRecordInterceptor((record, consumer) -> {
            byte[] bytes = record.value();
            String payload = bytes == null ? "" : new String(bytes, StandardCharsets.UTF_8);
            String type = extractType(payload, record.topic());
            store.addConsumed(new EventEntry(record.topic(), record.key(), type, payload, Instant.now()));
            return record;
        });
    }

    private static String extractType(String json, String topic) {
        // OrderEvent subtypes carry "eventType"
        String eventType = extractField(json, "eventType");
        if (eventType != null) return eventType;
        // Commands carry "action" — prefix with the command class name inferred from topic
        String action = extractField(json, "action");
        if (action != null) {
            if (topic.contains("inventory")) return "InventoryCommand[" + action + "]";
            if (topic.contains("payment"))   return "PaymentCommand[" + action + "]";
        }
        return "unknown";
    }

    private static String extractField(String json, String field) {
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx == -1) return null;
        int colon = json.indexOf(':', idx + key.length());
        if (colon == -1) return null;
        int start = json.indexOf('"', colon + 1);
        if (start == -1) return null;
        int end = json.indexOf('"', start + 1);
        if (end == -1) return null;
        return json.substring(start + 1, end);
    }
}
