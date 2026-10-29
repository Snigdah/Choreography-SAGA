package com.example.gl.entity;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(
        name = "gl_journal",
        // one POSTING and at most one REVERSAL per transaction
        uniqueConstraints = @UniqueConstraint(
                name = "uk_gl_transaction_ref_type",
                columnNames = {"transaction_ref", "journal_type"}
        )
)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GlJournal {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "transaction_ref", nullable = false, length = 50)
    private String transactionRef;

    @Enumerated(EnumType.STRING)
    @Column(name = "journal_type", nullable = false, length = 20)
    private JournalType journalType;

    @Enumerated(EnumType.STRING)
    @Column(name = "posting_status", nullable = false, length = 30)
    private PostingStatus postingStatus;

    /** For a REVERSAL journal: id of the POSTING journal it reverses. */
    @Column(name = "reversal_of")
    private UUID reversalOf;

    @Column(nullable = false, precision = 19, scale = 4)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(length = 500)
    private String reason;

    @Version
    private Long version;

    @Column(name = "posted_at")
    private LocalDateTime postedAt;

    @PrePersist
    protected void onCreate() {
        if (postedAt == null) {
            postedAt = LocalDateTime.now();
        }
    }
}
