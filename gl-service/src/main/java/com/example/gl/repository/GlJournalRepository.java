package com.example.gl.repository;

import com.example.gl.entity.GlJournal;
import com.example.gl.entity.JournalType;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface GlJournalRepository extends JpaRepository<GlJournal, UUID> {

    Optional<GlJournal> findByTransactionRefAndJournalType(String transactionRef, JournalType journalType);

    List<GlJournal> findByTransactionRefOrderByPostedAtAsc(String transactionRef);
}
