package com.example.compliance.messaging;

import com.example.compliance.messaging.event.EventBodies;
import com.example.compliance.messaging.event.EventMessage;
import com.example.compliance.messaging.event.EventType;
import com.example.compliance.messaging.event.FlowType;
import com.example.compliance.service.ComplianceService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Compliance takes part only in Scenario 2, in parallel with GL. */
@Slf4j
@Component
@RequiredArgsConstructor
public class ComplianceEventListener {

    private final ComplianceService complianceService;
    private final EventBodyReader reader;

    @KafkaListener(topics = KafkaTopics.ACCOUNT_EVENTS)
    public void onEvent(ConsumerRecord<String, EventMessage> record) {
        EventMessage msg = record.value();
        if (EventType.of(msg.getEventType()) != EventType.FUNDS_RESERVED) {
            return; // FUNDS_RELEASED etc. are not relevant here
        }

        EventContext.set(msg.getEventId());
        try {
            var body = reader.read(msg, EventBodies.FundsReserved.class);
            if (body.flowType() == FlowType.PARALLEL) {
                log.info("[KAFKA] <<< RECEIVED   {} | ref={} | from={} | topic={} partition={} offset={}",
                        msg.getEventType(), msg.getCorrelationId(), msg.getSource(),
                        record.topic(), record.partition(), record.offset());
                complianceService.checkTransaction(body);
            }
        } finally {
            EventContext.clear();
        }
    }
}
