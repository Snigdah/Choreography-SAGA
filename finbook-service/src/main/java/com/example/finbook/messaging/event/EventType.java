package com.example.finbook.messaging.event;

/** All events of both Saga scenarios. The body class of each type is in {@link EventBodies}. */
public enum EventType {

    TRANSACTION_INITIATED,     // body: TransactionInitiated

    FUNDS_RESERVED,            // body: FundsReserved
    FUNDS_RELEASED,            // body: FundsReleased            (compensation)

    GL_POSTING_COMPLETED,      // body: GlPostingCompleted
    GL_POSTING_FAILED,         // body: GlPostingFailed
    GL_POSTING_REVERSED,       // body: GlPostingReversed        (compensation)
    GL_POSTING_CANCELLED,      // body: GlPostingCancelled       (compensation)

    COMPLIANCE_APPROVED,       // body: ComplianceApproved
    COMPLIANCE_REJECTED,       // body: ComplianceRejected

    FINBOOK_ENTRY_CREATED,     // body: FinBookEntryCreated
    FINBOOK_ENTRY_FAILED,      // body: FinBookEntryFailed

    /** Any eventType this service does not know (for example, added later by another team). */
    UNKNOWN;

    /** Tolerant reader: never throws for a new or misspelled type. */
    public static EventType of(String value) {
        if (value == null) {
            return UNKNOWN;
        }
        try {
            return valueOf(value);
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
