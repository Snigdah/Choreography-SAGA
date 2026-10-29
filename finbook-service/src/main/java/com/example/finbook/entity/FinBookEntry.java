package com.example.finbook.entity;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(
        name = "finbook_entry",
        uniqueConstraints = @UniqueConstraint(name = "uk_finbook_transaction_ref", columnNames = "transaction_ref")
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FinBookEntry {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "transaction_ref", nullable = false, length = 50)
    private String transactionRef;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "entry_type", nullable = false, length = 30)
    private String entryType;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 30)
    private EntryStatus status;

    @Column(length = 500)
    private String reason;

    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        if (createdAt == null) {
            createdAt = LocalDateTime.now();
        }
    }
}
