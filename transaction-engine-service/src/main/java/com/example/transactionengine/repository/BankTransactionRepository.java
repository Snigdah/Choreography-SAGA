package com.example.transactionengine.repository;

import com.example.transactionengine.entity.BankTransaction;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface BankTransactionRepository extends JpaRepository<BankTransaction, UUID> {

    Optional<BankTransaction> findByTransactionRef(String transactionRef);
}
