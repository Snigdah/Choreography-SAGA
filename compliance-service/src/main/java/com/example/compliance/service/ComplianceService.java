package com.example.compliance.service;

import com.example.compliance.dto.response.ComplianceCheckResponse;
import com.example.compliance.entity.ComplianceCheck;
import com.example.compliance.entity.Decision;
import com.example.compliance.messaging.KafkaTopics;
import com.example.compliance.messaging.SagaEventPublisher;
import com.example.compliance.messaging.event.EventBodies;
import com.example.compliance.messaging.event.EventType;
import com.example.compliance.repository.ComplianceCheckRepository;
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
public class ComplianceService {

    private static final String RULE_TXN_LIMIT = "SINGLE_TXN_LIMIT";

    private final ComplianceCheckRepository repository;
    private final SagaEventPublisher publisher;

    @Value("${saga.rules.compliance.max-transaction-amount}")
    private BigDecimal maxTransactionAmount;

    /** FUNDS_RESERVED (Scenario 2) -> COMPLIANCE_APPROVED or COMPLIANCE_REJECTED */
    @Transactional
    public void checkTransaction(EventBodies.FundsReserved in) {
        if (repository.findByTransactionRef(in.transactionRef()).isPresent()) {
            log.info("[COMPLIANCE] {} already checked - duplicate event ignored", in.transactionRef());
            return;
        }

        // Business rule: single-transaction limit
        boolean rejected = in.amount().compareTo(maxTransactionAmount) > 0;
        String reason = rejected
                ? "Single-transaction limit exceeded: " + in.amount() + " > " + maxTransactionAmount
                : null;

        ComplianceCheck check = repository.save(ComplianceCheck.builder()
                .transactionRef(in.transactionRef())
                .amount(in.amount())
                .decision(rejected ? Decision.REJECTED : Decision.APPROVED)
                .ruleCode(rejected ? RULE_TXN_LIMIT : null)
                .reason(reason)
                .build());

        if (rejected) {
            log.warn("[COMPLIANCE] {} REJECTED | {}", in.transactionRef(), reason);
            publisher.publish(KafkaTopics.COMPLIANCE_EVENTS, EventType.COMPLIANCE_REJECTED,
                    new EventBodies.ComplianceRejected(in.transactionRef(), in.flowType(), check.getId(),
                            RULE_TXN_LIMIT, in.amount(), in.currency(), reason));
        } else {
            log.info("[COMPLIANCE] {} APPROVED", in.transactionRef());
            publisher.publish(KafkaTopics.COMPLIANCE_EVENTS, EventType.COMPLIANCE_APPROVED,
                    new EventBodies.ComplianceApproved(in.transactionRef(), in.flowType(), check.getId(),
                            in.amount(), in.currency()));
        }
    }

    @Transactional(readOnly = true)
    public ComplianceCheckResponse getCheck(String transactionRef) {
        ComplianceCheck c = repository.findByTransactionRef(transactionRef)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Compliance check not found: " + transactionRef));
        return ComplianceCheckResponse.builder()
                .id(c.getId())
                .transactionRef(c.getTransactionRef())
                .amount(c.getAmount())
                .decision(c.getDecision().name())
                .ruleCode(c.getRuleCode())
                .reason(c.getReason())
                .checkedAt(c.getCheckedAt())
                .build();
    }
}
