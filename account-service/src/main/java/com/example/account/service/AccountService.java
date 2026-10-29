package com.example.account.service;

import com.example.account.dto.response.ReservationResponse;
import com.example.account.entity.AccountReservation;
import com.example.account.entity.ReservationStatus;
import com.example.account.messaging.KafkaTopics;
import com.example.account.messaging.SagaEventPublisher;
import com.example.account.messaging.event.EventBodies;
import com.example.account.messaging.event.EventType;
import com.example.account.messaging.event.FlowType;
import com.example.account.repository.AccountReservationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountService {

    private final AccountReservationRepository repository;
    private final SagaEventPublisher publisher;

    /** TRANSACTION_INITIATED (Scenario 2) -> put the money on hold -> FUNDS_RESERVED */
    @Transactional
    public void reserveFunds(EventBodies.TransactionInitiated in) {
        if (repository.findByTransactionRef(in.transactionRef()).isPresent()) {
            log.info("[ACCOUNT] {} already reserved - duplicate event ignored", in.transactionRef());
            return;
        }

        AccountReservation reservation = repository.save(AccountReservation.builder()
                .transactionRef(in.transactionRef())
                .accountNumber(in.debitAccount())
                .reservedAmount(in.amount())
                .currency(in.currency())
                .status(ReservationStatus.RESERVED)
                .build());

        log.info("[ACCOUNT] {} RESERVED {} {} on account {}",
                in.transactionRef(), in.amount(), in.currency(), in.debitAccount());
        publisher.publish(KafkaTopics.ACCOUNT_EVENTS, EventType.FUNDS_RESERVED,
                new EventBodies.FundsReserved(in.transactionRef(), in.flowType(), reservation.getId(),
                        in.debitAccount(), in.creditAccount(), in.amount(), in.currency()));
    }

    /** COMPENSATION: a later step failed -> remove the hold -> FUNDS_RELEASED */
    @Transactional
    public void releaseFunds(String transactionRef, FlowType flowType, EventType cause, String reason) {
        Optional<AccountReservation> found = repository.findByTransactionRef(transactionRef);
        if (found.isEmpty()) {
            log.warn("[ACCOUNT] {} nothing reserved - nothing to release", transactionRef);
            return;
        }
        AccountReservation reservation = found.get();
        if (reservation.getStatus() != ReservationStatus.RESERVED) {
            log.info("[ACCOUNT] {} already {} - ignore {}", transactionRef, reservation.getStatus(), cause);
            return;
        }

        reservation.setStatus(ReservationStatus.RELEASED);
        reservation.setReason(cause + ": " + reason);

        log.warn("[ACCOUNT] {} RELEASED {} {} (compensation, cause={})",
                transactionRef, reservation.getReservedAmount(), reservation.getCurrency(), cause);
        publisher.publish(KafkaTopics.ACCOUNT_EVENTS, EventType.FUNDS_RELEASED,
                new EventBodies.FundsReleased(transactionRef, flowType, reservation.getId(),
                        reservation.getReservedAmount(), reservation.getCurrency(), reason));
    }

    /** FINBOOK_ENTRY_CREATED (Scenario 2) -> Saga succeeded -> hold becomes final. */
    @Transactional
    public void confirmReservation(String transactionRef) {
        repository.findByTransactionRef(transactionRef)
                .filter(r -> r.getStatus() == ReservationStatus.RESERVED)
                .ifPresent(r -> {
                    r.setStatus(ReservationStatus.CONFIRMED);
                    log.info("[ACCOUNT] {} CONFIRMED", transactionRef);
                });
    }

    @Transactional(readOnly = true)
    public ReservationResponse getReservation(String transactionRef) {
        AccountReservation r = repository.findByTransactionRef(transactionRef)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "Reservation not found: " + transactionRef));
        return ReservationResponse.builder()
                .id(r.getId())
                .transactionRef(r.getTransactionRef())
                .accountNumber(r.getAccountNumber())
                .reservedAmount(r.getReservedAmount())
                .currency(r.getCurrency())
                .status(r.getStatus().name())
                .reason(r.getReason())
                .createdAt(r.getCreatedAt())
                .updatedAt(r.getUpdatedAt())
                .build();
    }
}
