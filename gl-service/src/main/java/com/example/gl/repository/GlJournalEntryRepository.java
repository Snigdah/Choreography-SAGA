package com.example.gl.repository;

import com.example.gl.entity.GlJournalEntry;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface GlJournalEntryRepository extends JpaRepository<GlJournalEntry, UUID> {

    List<GlJournalEntry> findByJournal_Id(UUID journalId);
}
