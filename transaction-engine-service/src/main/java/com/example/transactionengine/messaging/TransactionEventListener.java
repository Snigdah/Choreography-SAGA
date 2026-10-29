package com.example.transactionengine.messaging;

import com.example.transactionengine.messaging.event.EventBodies;
import com.example.transactionengine.messaging.event.EventMessage;
import com.example.transactionengine.messaging.event.EventType;
import com.example.transactionengine.service.TransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Transaction Engine watches every service's events to know how the Saga ends.
 * Step 1: read eventType from the envelope.  Step 2: read the body of that type.  Step 3: call the service.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionEventListener {

    private final TransactionService transactionService;
    private final EventBodyReader reader;

    @KafkaListener(topics = {
            KafkaTopics.ACCOUNT_EVENTS,
            KafkaTopics.GL_EVENTS,
            KafkaTopics.COMPLIANCE_EVENTS,
            KafkaTopics.FINBOOK_EVENTS
    })
    public void onEvent(ConsumerRecord<String, EventMessage> record) {
        EventMessage msg = record.value();
        EventType type = EventType.of(msg.getEventType());
        log.info("[KAFKA] <<< RECEIVED   {} | ref={} | from={} | topic={} partition={} offset={}",
                msg.getEventType(), msg.getCorrelationId(), msg.getSource(), record.topic(), record.partition(), record.offset());

        EventContext.set(msg.getEventId());
        try {
            switch (type) {
                case FINBOOK_ENTRY_CREATED -> {
                    var body = reader.read(msg, EventBodies.FinBookEntryCreated.class);
                    transactionService.completeTransaction(body.transactionRef());
                }
                case GL_POSTING_FAILED -> {
                    var body = reader.read(msg, EventBodies.GlPostingFailed.class);
                    transactionService.glPostingFailed(body.transactionRef(), body.reason());
                }
                case FINBOOK_ENTRY_FAILED -> {
                    var body = reader.read(msg, EventBodies.FinBookEntryFailed.class);
                    transactionService.stepFailed(body.transactionRef(), type, body.reason());
                }
                case COMPLIANCE_REJECTED -> {
                    var body = reader.read(msg, EventBodies.ComplianceRejected.class);
                    transactionService.stepFailed(body.transactionRef(), type, body.reason());
                }
                case GL_POSTING_REVERSED -> {
                    var body = reader.read(msg, EventBodies.GlPostingReversed.class);
                    transactionService.glCompensated(body.transactionRef(), type);
                }
                case GL_POSTING_CANCELLED -> {
                    var body = reader.read(msg, EventBodies.GlPostingCancelled.class);
                    transactionService.glCompensated(body.transactionRef(), type);
                }
                case FUNDS_RELEASED -> {
                    var body = reader.read(msg, EventBodies.FundsReleased.class);
                    transactionService.fundsReleased(body.transactionRef());
                }
                case UNKNOWN -> log.warn("[SAGA] unknown eventType '{}' from {} - ignored", msg.getEventType(), msg.getSource());
                default -> log.info("[SAGA] {} progress: {}", msg.getCorrelationId(), type);
            }
        } finally {
            EventContext.clear();
        }
    }
}
