package com.example.gl.messaging;

import com.example.gl.messaging.event.EventBodies;
import com.example.gl.messaging.event.EventMessage;
import com.example.gl.messaging.event.EventType;
import com.example.gl.messaging.event.FlowType;
import com.example.gl.service.GlService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class GlEventListener {

    private final GlService glService;
    private final EventBodyReader reader;

    @KafkaListener(topics = {
            KafkaTopics.TRANSACTION_EVENTS,
            KafkaTopics.ACCOUNT_EVENTS,
            KafkaTopics.COMPLIANCE_EVENTS,
            KafkaTopics.FINBOOK_EVENTS
    })
    public void onEvent(ConsumerRecord<String, EventMessage> record) {
        EventMessage msg = record.value();
        EventType type = EventType.of(msg.getEventType());

        EventContext.set(msg.getEventId());
        try {
            switch (type) {
                // Scenario 1 starts GL directly
                case TRANSACTION_INITIATED -> {
                    var b = reader.read(msg, EventBodies.TransactionInitiated.class);
                    if (b.flowType() == FlowType.SEQUENTIAL) {
                        logReceived(record);
                        glService.postJournal(b.transactionRef(), b.flowType(), b.debitAccount(), b.creditAccount(),
                                b.amount(), b.currency());
                    }
                }
                // Scenario 2 starts GL after funds are reserved
                case FUNDS_RESERVED -> {
                    var b = reader.read(msg, EventBodies.FundsReserved.class);
                    if (b.flowType() == FlowType.PARALLEL) {
                        logReceived(record);
                        glService.postJournal(b.transactionRef(), b.flowType(), b.debitAccount(), b.creditAccount(),
                                b.amount(), b.currency());
                    }
                }
                // Compensation triggers
                case FINBOOK_ENTRY_FAILED -> {
                    var b = reader.read(msg, EventBodies.FinBookEntryFailed.class);
                    logReceived(record);
                    glService.reverseJournal(b.transactionRef(), b.flowType(), type, b.reason(), b.amount(), b.currency());
                }
                case COMPLIANCE_REJECTED -> {
                    var b = reader.read(msg, EventBodies.ComplianceRejected.class);
                    if (b.flowType() == FlowType.PARALLEL) {
                        logReceived(record);
                        glService.reverseJournal(b.transactionRef(), b.flowType(), type, b.reason(), b.amount(), b.currency());
                    }
                }
                default -> { /* not relevant for GL */ }
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
