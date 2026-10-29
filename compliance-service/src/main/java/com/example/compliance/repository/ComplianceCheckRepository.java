package com.example.compliance.repository;

import com.example.compliance.entity.ComplianceCheck;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface ComplianceCheckRepository extends JpaRepository<ComplianceCheck, UUID> {

    Optional<ComplianceCheck> findByTransactionRef(String transactionRef);
}
