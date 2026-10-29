package com.example.compliance.messaging.event;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * The "body" of each event type. One record per eventType, carrying only what consumers need.
 * Rules: never remove or rename a field; add new fields only (consumers ignore unknown fields).
 * A breaking change means a new eventVersion.
 */
public final class EventBodies {

    public static final int VERSION = 1;

    private EventBodies() {
    }

    /** Every body has these two fields. */
    public interface SagaBody {
        String transactionRef();

        FlowType flowType();
    }

    // ---------------- Transaction Engine ----------------
    public record TransactionInitiated(String transactionRef, FlowType flowType,
                                       String debitAccount, String creditAccount,
                                       BigDecimal amount, String currency) implements SagaBody {
    }

    // ---------------- Account ----------------
    public record FundsReserved(String transactionRef, FlowType flowType, UUID reservationId,
                                String debitAccount, String creditAccount,
                                BigDecimal amount, String currency) implements SagaBody {
    }

    public record FundsReleased(String transactionRef, FlowType flowType, UUID reservationId,
                                BigDecimal amount, String currency, String reason) implements SagaBody {
    }

    // ---------------- GL ----------------
    public record GlPostingCompleted(String transactionRef, FlowType flowType, UUID journalId,
                                     String debitAccount, String creditAccount,
                                     BigDecimal amount, String currency) implements SagaBody {
    }

    public record GlPostingFailed(String transactionRef, FlowType flowType,
                                  BigDecimal amount, String currency, String reason) implements SagaBody {
    }

    public record GlPostingReversed(String transactionRef, FlowType flowType,
                                    UUID originalJournalId, UUID reversalJournalId,
                                    BigDecimal amount, String currency, String reason) implements SagaBody {
    }

    public record GlPostingCancelled(String transactionRef, FlowType flowType, String reason) implements SagaBody {
    }

    // ---------------- Compliance ----------------
    public record ComplianceApproved(String transactionRef, FlowType flowType, UUID checkId,
                                     BigDecimal amount, String currency) implements SagaBody {
    }

    public record ComplianceRejected(String transactionRef, FlowType flowType, UUID checkId, String ruleCode,
                                     BigDecimal amount, String currency, String reason) implements SagaBody {
    }

    // ---------------- FinBook ----------------
    public record FinBookEntryCreated(String transactionRef, FlowType flowType, UUID entryId,
                                      BigDecimal amount, String currency) implements SagaBody {
    }

    public record FinBookEntryFailed(String transactionRef, FlowType flowType,
                                     BigDecimal amount, String currency, String reason) implements SagaBody {
    }
}
