package com.example.finbook.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

/**
 * Scenario 2 "join": GL and Compliance run in parallel, FinBook waits for BOTH.
 * One row per transaction remembers which results have arrived.
 */
@Entity
@Table(name = "finbook_join")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FinBookJoin {

    @Id
    @Column(name = "transaction_ref", length = 50)
    private String transactionRef;

    @Column(name = "gl_done", nullable = false)
    private boolean glDone;

    @Column(name = "compliance_done", nullable = false)
    private boolean complianceDone;

    /** GL failed or Compliance rejected -> FinBook must never run. */
    @Column(nullable = false)
    private boolean rejected;

    @Version
    private Long version;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    @PreUpdate
    protected void touch() {
        updatedAt = LocalDateTime.now();
    }
}
