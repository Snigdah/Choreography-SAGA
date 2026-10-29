package com.example.account.repository;

import com.example.account.entity.AccountReservation;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface AccountReservationRepository extends JpaRepository<AccountReservation, UUID> {

    Optional<AccountReservation> findByTransactionRef(String transactionRef);
}
