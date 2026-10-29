package com.example.finbook.service;

import com.example.finbook.dto.response.FinBookEntryResponse;
import com.example.finbook.entity.EntryStatus;
import com.example.finbook.entity.FinBookEntry;
import com.example.finbook.entity.FinBookJoin;
import com.example.finbook.messaging.KafkaTopics;
import com.example.finbook.messaging.SagaEventPublisher;
import com.example.finbook.messaging.event.EventBodies;
import com.example.finbook.messaging.event.EventType;
import com.example.finbook.messaging.event.FlowType;
import com.example.finbook.repository.FinBookEntryRepository;
import com.example.finbook.repository.FinBookJoinRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;

@Slf4j
@Service
@RequiredArgsConstructor
public class FinBookService {

    private static final String ENTRY_TYPE = "FUND_TRANSFER";

    private final FinBookEntryRepository entryRepository;
    private final FinBookJoinRepository joinRepository;
    private final SagaEventPublisher publisher;

    @Value("${saga.rules.finbook.max-entry-amount}")
    private BigDecimal maxEntryAmount;

    // ------------------------------------------------------------------
    // Scenario 1: GL_POSTING_COMPLETED -> create entry
    // ------------------------------------------------------------------
    @Transactional
    public void createEntry(String transactionRef, FlowType flowType, BigDecimal amount, String currency) {
        if (entryRepository.findByTransactionRef(transactionRef).isPresent()) {
            log.info("[FINBOOK] {} entry already exists - duplicate event ignored", transactionRef);
            return;
        }

        // Business rule: FinBook entry limit
        if (amount.compareTo(maxEntryAmount) > 0) {
            String reason = "FinBook entry limit exceeded: " + amount + " > " + maxEntryAmount;
            entryRepository.save(entry(transactionRef, amount, currency, EntryStatus.FAILED, reason));
            log.warn("[FINBOOK] {} entry FAILED | {}", transactionRef, reason);
            publisher.publish(KafkaTopics.FINBOOK_EVENTS, EventType.FINBOOK_ENTRY_FAILED,
                    new EventBodies.FinBookEntryFailed(transactionRef, flowType, amount, currency, reason));
            return;
        }

        FinBookEntry saved = entryRepository.save(entry(transactionRef, amount, currency, EntryStatus.CREATED, null));
        log.info("[FINBOOK] {} entry CREATED | {} {}", transactionRef, amount, currency);
        publisher.publish(KafkaTopics.FINBOOK_EVENTS, EventType.FINBOOK_ENTRY_CREATED,
                new EventBodies.FinBookEntryCreated(transactionRef, flowType, saved.getId(), amount, currency));
    }

    // ------------------------------------------------------------------
    // Scenario 2: wait for BOTH GL and Compliance (the "join")
    // ------------------------------------------------------------------

    /** GL_POSTING_COMPLETED (Scenario 2) */
    @Transactional
    public void glCompleted(String transactionRef, FlowType flowType, BigDecimal amount, String currency) {
        FinBookJoin join = loadJoin(transactionRef);
        join.setGlDone(true);
        tryCreateEntry(join, flowType, amount, currency);
    }

    /** COMPLIANCE_APPROVED (Scenario 2) */
    @Transactional
    public void complianceApproved(String transactionRef, FlowType flowType, BigDecimal amount, String currency) {
        FinBookJoin join = loadJoin(transactionRef);
        join.setComplianceDone(true);
        tryCreateEntry(join, flowType, amount, currency);
    }

    /** GL_POSTING_FAILED / COMPLIANCE_REJECTED (Scenario 2) -> FinBook must NOT run. */
    @Transactional
    public void parallelStepFailed(String transactionRef, EventType cause) {
        FinBookJoin join = loadJoin(transactionRef);
        join.setRejected(true);
        joinRepository.save(join);
        log.warn("[FINBOOK] {} will NOT create entry - {}", transactionRef, cause);
    }

    private void tryCreateEntry(FinBookJoin join, FlowType flowType, BigDecimal amount, String currency) {
        joinRepository.save(join);
        String ref = join.getTransactionRef();
        if (join.isRejected()) {
            log.info("[FINBOOK] {} join already rejected - skip", ref);
        } else if (join.isGlDone() && join.isComplianceDone()) {
            log.info("[FINBOOK] {} GL + Compliance both done -> creating entry", ref);
            createEntry(ref, flowType, amount, currency);
        } else {
            log.info("[FINBOOK] {} waiting | glDone={} complianceDone={}", ref, join.isGlDone(), join.isComplianceDone());
        }
    }

    private FinBookJoin loadJoin(String transactionRef) {
        return joinRepository.findById(transactionRef)
                .orElseGet(() -> FinBookJoin.builder().transactionRef(transactionRef).build());
    }

    // ------------------------------------------------------------------

    @Transactional(readOnly = true)
    public FinBookEntryResponse getEntry(String transactionRef) {
        FinBookEntry e = entryRepository.findByTransactionRef(transactionRef)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "FinBook entry not found: " + transactionRef));
        return FinBookEntryResponse.builder()
                .id(e.getId())
                .transactionRef(e.getTransactionRef())
                .amount(e.getAmount())
                .currency(e.getCurrency())
                .entryType(e.getEntryType())
                .status(e.getStatus().name())
                .reason(e.getReason())
                .createdAt(e.getCreatedAt())
                .build();
    }

    private FinBookEntry entry(String transactionRef, BigDecimal amount, String currency, EntryStatus status, String reason) {
        return FinBookEntry.builder()
                .transactionRef(transactionRef)
                .amount(amount)
                .currency(currency)
                .entryType(ENTRY_TYPE)
                .status(status)
                .reason(reason)
                .build();
    }
}
