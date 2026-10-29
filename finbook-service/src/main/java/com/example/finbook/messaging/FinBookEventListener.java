package com.example.finbook.messaging;

import com.example.finbook.messaging.event.EventBodies;
import com.example.finbook.messaging.event.EventMessage;
import com.example.finbook.messaging.event.EventType;
import com.example.finbook.messaging.event.FlowType;
import com.example.finbook.service.FinBookService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * One listener for both topics: events of the same transaction (same key, same partition number)
 * are handled by the same consumer thread, so the Scenario 2 join is never updated concurrently.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FinBookEventListener {

    private final FinBookService finBookService;
    private final EventBodyReader reader;

    @KafkaListener(topics = {KafkaTopics.GL_EVENTS, KafkaTopics.COMPLIANCE_EVENTS})
    public void onEvent(ConsumerRecord<String, EventMessage> record) {
        EventMessage msg = record.value();
        EventType type = EventType.of(msg.getEventType());

        EventContext.set(msg.getEventId());
        try {
            switch (type) {
                case GL_POSTING_COMPLETED -> {
                    var b = reader.read(msg, EventBodies.GlPostingCompleted.class);
                    logReceived(record);
                    if (b.flowType() == FlowType.SEQUENTIAL) {
                        finBookService.createEntry(b.transactionRef(), b.flowType(), b.amount(), b.currency());   // Scenario 1
                    } else {
                        finBookService.glCompleted(b.transactionRef(), b.flowType(), b.amount(), b.currency());   // Scenario 2 join
                    }
                }
                case COMPLIANCE_APPROVED -> {
                    var b = reader.read(msg, EventBodies.ComplianceApproved.class);
                    logReceived(record);
                    finBookService.complianceApproved(b.transactionRef(), b.flowType(), b.amount(), b.currency());
                }
                case GL_POSTING_FAILED -> {
                    var b = reader.read(msg, EventBodies.GlPostingFailed.class);
                    if (b.flowType() == FlowType.PARALLEL) {
                        logReceived(record);
                        finBookService.parallelStepFailed(b.transactionRef(), type);
                    }
                }
                case COMPLIANCE_REJECTED -> {
                    var b = reader.read(msg, EventBodies.ComplianceRejected.class);
                    if (b.flowType() == FlowType.PARALLEL) {
                        logReceived(record);
                        finBookService.parallelStepFailed(b.transactionRef(), type);
                    }
                }
                default -> { /* not relevant for FinBook */ }
            }
        } finally {
            EventContext.clear();
        }
    }

    private void logReceived(ConsumerRecord<String, EventMessage> record) {
        log.info("[KAFKA] <<< RECEIVED   {} | ref={} | from={} | topic={} partition={} offset={}",
                record.value().getEventType(), record.value().getCorrelationId(), record.value().getSource(),
                record.topic(), record.partition(), record.offset());
    }
}
