package com.example.transactionengine.messaging;

import com.example.transactionengine.messaging.event.EventBodies;
import com.example.transactionengine.messaging.event.EventMessage;
import com.example.transactionengine.messaging.event.EventType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

/**
 * Wraps a body in the standard envelope and publishes it.
 * <ul>
 *   <li>Kafka key = transactionRef, so all events of one transfer stay in order.</li>
 *   <li>eventType, eventVersion and source are also sent as Kafka headers,
 *       so consumers and tools can filter without reading the JSON.</li>
 *   <li>Inside a DB transaction, the event is sent only AFTER commit.
 *       (No Outbox in this POC: commit OK + Kafka down = event lost.)</li>
 * </ul>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SagaEventPublisher {

    public static final String HEADER_EVENT_TYPE = "eventType";
    public static final String HEADER_EVENT_VERSION = "eventVersion";
    public static final String HEADER_SOURCE = "source";

    private final KafkaTemplate<String, EventMessage> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${spring.application.name}")
    private String source;

    public void publish(String topic, EventType type, EventBodies.SagaBody body) {
        EventMessage message = EventMessage.builder()
                .eventId(UUID.randomUUID())
                .eventType(type.name())
                .eventVersion(EventBodies.VERSION)
                .source(source)
                .correlationId(body.transactionRef())
                .causationId(EventContext.currentEventId())
                .occurredAt(Instant.now())
                .body(objectMapper.valueToTree(body))
                .build();

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    send(topic, message);
                }
            });
        } else {
            send(topic, message);
        }
    }

    private void send(String topic, EventMessage message) {
        ProducerRecord<String, EventMessage> record =
                new ProducerRecord<>(topic, message.getCorrelationId(), message);
        record.headers()
                .add(HEADER_EVENT_TYPE, bytes(message.getEventType()))
                .add(HEADER_EVENT_VERSION, bytes(String.valueOf(message.getEventVersion())))
                .add(HEADER_SOURCE, bytes(message.getSource()));

        kafkaTemplate.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[KAFKA] >>> PUBLISH FAILED  {} | ref={} | topic={} | error={}",
                        message.getEventType(), message.getCorrelationId(), topic, ex.getMessage(), ex);
            } else {
                log.info("[KAFKA] >>> PUBLISHED  {} | ref={} | topic={} partition={} offset={}",
                        message.getEventType(), message.getCorrelationId(), topic,
                        result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
            }
        });
    }

    private static byte[] bytes(String value) {
        return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
    }
}
