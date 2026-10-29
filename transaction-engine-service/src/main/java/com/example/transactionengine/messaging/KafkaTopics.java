package com.example.transactionengine.messaging;

import java.util.List;

/** Saga topic names. One topic per producing service. */
public final class KafkaTopics {

    public static final String TRANSACTION_EVENTS = "transaction-events";
    public static final String ACCOUNT_EVENTS = "account-events";
    public static final String GL_EVENTS = "gl-events";
    public static final String COMPLIANCE_EVENTS = "compliance-events";
    public static final String FINBOOK_EVENTS = "finbook-events";

    /** Topics this service produces to or consumes from - created at startup if missing. */
    public static final List<String> USED_BY_THIS_SERVICE = List.of(TRANSACTION_EVENTS, ACCOUNT_EVENTS, GL_EVENTS, COMPLIANCE_EVENTS, FINBOOK_EVENTS);

    private KafkaTopics() {
    }
}
