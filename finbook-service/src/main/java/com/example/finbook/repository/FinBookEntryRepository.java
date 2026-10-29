package com.example.finbook.repository;

import com.example.finbook.entity.FinBookEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface FinBookEntryRepository extends JpaRepository<FinBookEntry, UUID> {

    Optional<FinBookEntry> findByTransactionRef(String transactionRef);
}
