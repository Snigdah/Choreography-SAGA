package com.example.transactionengine.messaging.event;

public enum FlowType {
    /** Scenario 1: Transaction Engine -> GL -> FinBook */
    SEQUENTIAL,
    /** Scenario 2: Transaction Engine -> Account -> (GL || Compliance) -> FinBook */
    PARALLEL
}
