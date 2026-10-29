package com.example.account.messaging;

import com.example.account.messaging.event.EventBodies;
import com.example.account.messaging.event.EventMessage;
import com.example.account.messaging.event.EventType;
import com.example.account.messaging.event.FlowType;
import com.example.account.service.AccountService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Account takes part only in Scenario 2 (PARALLEL). */
@Slf4j
@Component
@RequiredArgsConstructor
public class AccountEventListener {

    private final AccountService accountService;
    private final EventBodyReader reader;

    @KafkaListener(topics = {
            KafkaTopics.TRANSACTION_EVENTS,
            KafkaTopics.GL_EVENTS,
            KafkaTopics.COMPLIANCE_EVENTS,
            KafkaTopics.FINBOOK_EVENTS
    })
    public void onEvent(ConsumerRecord<String, EventMessage> record) {
        EventMessage msg = record.value();
        EventType type = EventType.of(msg.getEventType());

        EventContext.set(msg.getEventId());
        try {
            switch (type) {
                case TRANSACTION_INITIATED -> {
                    var body = reader.read(msg, EventBodies.TransactionInitiated.class);
                    if (isParallel(body)) {
                        logReceived(record);
                        accountService.reserveFunds(body);
                    }
                }
                case GL_POSTING_FAILED -> {
                    var body = reader.read(msg, EventBodies.GlPostingFailed.class);
                    if (isParallel(body)) {
                        logReceived(record);
                        accountService.releaseFunds(body.transactionRef(), body.flowType(), type, body.reason());
                    }
                }
                case COMPLIANCE_REJECTED -> {
                    var body = reader.read(msg, EventBodies.ComplianceRejected.class);
                    if (isParallel(body)) {
                        logReceived(record);
                        accountService.releaseFunds(body.transactionRef(), body.flowType(), type, body.reason());
                    }
                }
                case FINBOOK_ENTRY_FAILED -> {
                    var body = reader.read(msg, EventBodies.FinBookEntryFailed.class);
                    if (isParallel(body)) {
                        logReceived(record);
                        accountService.releaseFunds(body.transactionRef(), body.flowType(), type, body.reason());
                    }
                }
                case FINBOOK_ENTRY_CREATED -> {
                    var body = reader.read(msg, EventBodies.FinBookEntryCreated.class);
                    if (isParallel(body)) {
                        logReceived(record);
                        accountService.confirmReservation(body.transactionRef());
                    }
                }
                default -> { /* not relevant for Account */ }
            }
        } finally {
            EventContext.clear();
        }
    }

    /** Scenario 1 does not involve Account. */
    private static boolean isParallel(EventBodies.SagaBody body) {
        return body.flowType() == FlowType.PARALLEL;
    }

    private void logReceived(ConsumerRecord<String, EventMessage> record) {
        log.info("[KAFKA] <<< RECEIVED   {} | ref={} | from={} | topic={} partition={} offset={}",
                record.value().getEventType(), record.value().getCorrelationId(), record.value().getSource(),
                record.topic(), record.partition(), record.offset());
    }
}
