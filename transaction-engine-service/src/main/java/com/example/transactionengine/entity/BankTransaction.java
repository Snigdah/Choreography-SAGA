package com.example.transactionengine.entity;

import com.example.transactionengine.messaging.event.FlowType;
import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(
        name = "bank_transaction",
        uniqueConstraints = @UniqueConstraint(name = "uk_transaction_ref", columnNames = "transaction_ref")
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class BankTransaction {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "transaction_ref", nullable = false, length = 50)
    private String transactionRef;

    @Enumerated(EnumType.STRING)
    @Column(name = "flow_type", nullable = false, length = 20)
    private FlowType flowType;

    @Column(name = "debit_account", nullable = false, length = 50)
    private String debitAccount;

    @Column(name = "credit_account", nullable = false, length = 50)
    private String creditAccount;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private TransactionStatus status;

    @Column(name = "status_reason", length = 500)
    private String statusReason;

    /** Compensation tracking: GL has nothing left to undo (reversed / cancelled / never posted). */
    @Column(name = "gl_settled", nullable = false)
    private boolean glSettled;

    /** Compensation tracking (Scenario 2): Account released the reserved funds. */
    @Column(name = "funds_released", nullable = false)
    private boolean fundsReleased;

    @Version
    private Long version;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = createdAt;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
