package com.example.transactionengine.service;

import com.example.transactionengine.dto.request.CreateTransactionRequest;
import com.example.transactionengine.dto.response.TransactionResponse;
import com.example.transactionengine.entity.BankTransaction;
import com.example.transactionengine.entity.TransactionStatus;
import com.example.transactionengine.messaging.KafkaTopics;
import com.example.transactionengine.messaging.SagaEventPublisher;
import com.example.transactionengine.messaging.event.EventBodies;
import com.example.transactionengine.messaging.event.EventType;
import com.example.transactionengine.messaging.event.FlowType;
import com.example.transactionengine.repository.BankTransactionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionService {

    private static final DateTimeFormatter REF_TIME = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private final BankTransactionRepository repository;
    private final SagaEventPublisher publisher;

    // ------------------------------------------------------------------
    // Step 1: client request -> save PENDING -> TRANSACTION_INITIATED
    // ------------------------------------------------------------------
    @Transactional
    public TransactionResponse createTransaction(CreateTransactionRequest request) {
        FlowType flowType = request.getFlowType() != null ? request.getFlowType() : FlowType.SEQUENTIAL;

        BankTransaction tx = repository.save(BankTransaction.builder()
                .transactionRef(newTransactionRef())
                .flowType(flowType)
                .debitAccount(request.getDebitAccount())
                .creditAccount(request.getCreditAccount())
                .amount(request.getAmount())
                .currency(request.getCurrency().toUpperCase())
                .status(TransactionStatus.PENDING)
                .build());

        log.info("[SAGA] {} STARTED | flow={} | {} {} | {} -> {}",
                tx.getTransactionRef(), flowType, tx.getAmount(), tx.getCurrency(),
                tx.getDebitAccount(), tx.getCreditAccount());

        publisher.publish(KafkaTopics.TRANSACTION_EVENTS, EventType.TRANSACTION_INITIATED,
                new EventBodies.TransactionInitiated(tx.getTransactionRef(), flowType,
                        tx.getDebitAccount(), tx.getCreditAccount(), tx.getAmount(), tx.getCurrency()));

        return toResponse(tx);
    }

    @Transactional(readOnly = true)
    public TransactionResponse getTransaction(String transactionRef) {
        return repository.findByTransactionRef(transactionRef)
                .map(this::toResponse)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Transaction not found: " + transactionRef));
    }

    // ------------------------------------------------------------------
    // Saga reactions (called by TransactionEventListener)
    // ------------------------------------------------------------------

    /** FINBOOK_ENTRY_CREATED -> the last step succeeded. */
    @Transactional
    public void completeTransaction(String transactionRef) {
        BankTransaction tx = find(transactionRef);
        if (tx.getStatus() != TransactionStatus.PENDING) {
            log.info("[SAGA] {} already {} - ignore completion", transactionRef, tx.getStatus());
            return;
        }
        tx.setStatus(TransactionStatus.COMPLETED);
        log.info("[SAGA] {} COMPLETED", transactionRef);
    }

    /** GL_POSTING_FAILED -> GL itself failed, so GL has nothing to undo. */
    @Transactional
    public void glPostingFailed(String transactionRef, String reason) {
        BankTransaction tx = find(transactionRef);
        tx.setGlSettled(true);
        startCompensation(tx, EventType.GL_POSTING_FAILED, reason);
    }

    /** FINBOOK_ENTRY_FAILED / COMPLIANCE_REJECTED -> earlier steps must be undone. */
    @Transactional
    public void stepFailed(String transactionRef, EventType cause, String reason) {
        startCompensation(find(transactionRef), cause, reason);
    }

    /** GL_POSTING_REVERSED / GL_POSTING_CANCELLED -> GL compensation finished. */
    @Transactional
    public void glCompensated(String transactionRef, EventType cause) {
        BankTransaction tx = find(transactionRef);
        tx.setGlSettled(true);
        log.info("[SAGA] {} compensation done by GL ({})", transactionRef, cause);
        finishIfCompensated(tx);
    }

    /** FUNDS_RELEASED -> Account compensation finished (Scenario 2). */
    @Transactional
    public void fundsReleased(String transactionRef) {
        BankTransaction tx = find(transactionRef);
        tx.setFundsReleased(true);
        log.info("[SAGA] {} compensation done by Account (FUNDS_RELEASED)", transactionRef);
        finishIfCompensated(tx);
    }

    // ------------------------------------------------------------------

    private void startCompensation(BankTransaction tx, EventType cause, String reason) {
        if (tx.getStatus() == TransactionStatus.PENDING) {
            tx.setStatus(TransactionStatus.COMPENSATING);
            tx.setStatusReason(cause + ": " + reason);
            log.warn("[SAGA] {} COMPENSATING | cause={} | reason={}", tx.getTransactionRef(), cause, reason);
        }
        finishIfCompensated(tx);
    }

    /**
     * Events can arrive in any order, so FAILED is set only when the Saga is
     * COMPENSATING and every required compensation has been reported.
     * Scenario 1: GL settled.  Scenario 2: GL settled AND funds released.
     */
    private void finishIfCompensated(BankTransaction tx) {
        if (tx.getStatus() != TransactionStatus.COMPENSATING) {
            return;
        }
        boolean done = tx.getFlowType() == FlowType.SEQUENTIAL
                ? tx.isGlSettled()
                : tx.isGlSettled() && tx.isFundsReleased();
        if (done) {
            tx.setStatus(TransactionStatus.FAILED);
            log.warn("[SAGA] {} FAILED - all compensations finished | {}", tx.getTransactionRef(), tx.getStatusReason());
        } else {
            log.info("[SAGA] {} waiting for compensation | glSettled={} fundsReleased={}",
                    tx.getTransactionRef(), tx.isGlSettled(), tx.isFundsReleased());
        }
    }

    private BankTransaction find(String transactionRef) {
        return repository.findByTransactionRef(transactionRef)
                .orElseThrow(() -> new IllegalStateException("Unknown transaction " + transactionRef));
    }

    private String newTransactionRef() {
        return "TXN-" + LocalDateTime.now().format(REF_TIME) + "-"
                + UUID.randomUUID().toString().substring(0, 6).toUpperCase();
    }

    private TransactionResponse toResponse(BankTransaction tx) {
        return TransactionResponse.builder()
                .id(tx.getId())
                .transactionRef(tx.getTransactionRef())
                .flowType(tx.getFlowType().name())
                .debitAccount(tx.getDebitAccount())
                .creditAccount(tx.getCreditAccount())
                .amount(tx.getAmount())
                .currency(tx.getCurrency())
                .status(tx.getStatus().name())
                .statusReason(tx.getStatusReason())
                .createdAt(tx.getCreatedAt())
                .updatedAt(tx.getUpdatedAt())
                .build();
    }
}
