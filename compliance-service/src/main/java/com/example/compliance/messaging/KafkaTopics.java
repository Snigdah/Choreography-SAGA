package com.example.compliance.messaging;

import java.util.List;

/** Saga topic names. One topic per producing service. */
public final class KafkaTopics {

    public static final String TRANSACTION_EVENTS = "transaction-events";
    public static final String ACCOUNT_EVENTS = "account-events";
    public static final String GL_EVENTS = "gl-events";
    public static final String COMPLIANCE_EVENTS = "compliance-events";
    public static final String FINBOOK_EVENTS = "finbook-events";

    /** Topics this service produces to or consumes from - created at startup if missing. */
    public static final List<String> USED_BY_THIS_SERVICE = List.of(ACCOUNT_EVENTS, COMPLIANCE_EVENTS);

    private KafkaTopics() {
    }
}
