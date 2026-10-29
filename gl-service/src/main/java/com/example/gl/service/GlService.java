package com.example.gl.service;

import com.example.gl.dto.response.GlPostingResponse;
import com.example.gl.entity.GlJournal;
import com.example.gl.entity.GlJournalEntry;
import com.example.gl.entity.JournalType;
import com.example.gl.entity.PostingStatus;
import com.example.gl.messaging.KafkaTopics;
import com.example.gl.messaging.SagaEventPublisher;
import com.example.gl.messaging.event.EventBodies;
import com.example.gl.messaging.event.EventType;
import com.example.gl.messaging.event.FlowType;
import com.example.gl.repository.GlJournalEntryRepository;
import com.example.gl.repository.GlJournalRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class GlService {

    private final GlJournalRepository journalRepository;
    private final GlJournalEntryRepository entryRepository;
    private final SagaEventPublisher publisher;

    @Value("${saga.rules.gl.max-posting-amount}")
    private BigDecimal maxPostingAmount;

    /**
     * Scenario 1: TRANSACTION_INITIATED   Scenario 2: FUNDS_RESERVED
     * -> book Dr debitAccount / Cr creditAccount -> GL_POSTING_COMPLETED (or GL_POSTING_FAILED)
     */
    @Transactional
    public void postJournal(String transactionRef, FlowType flowType, String debitAccount, String creditAccount,
                            BigDecimal amount, String currency) {
        Optional<GlJournal> existing = journalRepository
                .findByTransactionRefAndJournalType(transactionRef, JournalType.POSTING);
        if (existing.isPresent()) {
            // duplicate event, or CANCELLED because compensation arrived first
            log.info("[GL] {} posting already {} - skip", transactionRef, existing.get().getPostingStatus());
            return;
        }

        // Business rule: posting limit
        if (amount.compareTo(maxPostingAmount) > 0) {
            String reason = "GL posting limit exceeded: " + amount + " > " + maxPostingAmount;
            journalRepository.save(journal(transactionRef, JournalType.POSTING, PostingStatus.FAILED, amount, currency, reason));
            log.warn("[GL] {} posting FAILED | {}", transactionRef, reason);
            publisher.publish(KafkaTopics.GL_EVENTS, EventType.GL_POSTING_FAILED,
                    new EventBodies.GlPostingFailed(transactionRef, flowType, amount, currency, reason));
            return;
        }

        GlJournal posting = journalRepository.save(
                journal(transactionRef, JournalType.POSTING, PostingStatus.POSTED, amount, currency, null));
        entryRepository.saveAll(List.of(
                line(posting, debitAccount, amount, BigDecimal.ZERO),
                line(posting, creditAccount, BigDecimal.ZERO, amount)));

        log.info("[GL] {} POSTED | Dr {} {} / Cr {} {}", transactionRef, debitAccount, amount, creditAccount, amount);
        publisher.publish(KafkaTopics.GL_EVENTS, EventType.GL_POSTING_COMPLETED,
                new EventBodies.GlPostingCompleted(transactionRef, flowType, posting.getId(),
                        debitAccount, creditAccount, amount, currency));
    }

    /**
     * COMPENSATION - Scenario 1: FINBOOK_ENTRY_FAILED   Scenario 2: COMPLIANCE_REJECTED / FINBOOK_ENTRY_FAILED
     * Never deletes. Books a new REVERSAL journal from GL's OWN stored lines, with debit and credit swapped.
     * amount/currency are used only if nothing was posted yet (CANCELLED marker).
     */
    @Transactional
    public void reverseJournal(String transactionRef, FlowType flowType, EventType cause, String reason,
                               BigDecimal amount, String currency) {
        Optional<GlJournal> found = journalRepository
                .findByTransactionRefAndJournalType(transactionRef, JournalType.POSTING);

        // Compensation arrived BEFORE we posted (possible in Scenario 2) -> block the future posting
        if (found.isEmpty()) {
            journalRepository.save(journal(transactionRef, JournalType.POSTING, PostingStatus.CANCELLED,
                    amount, currency, "Cancelled before posting: " + reason));
            log.warn("[GL] {} CANCELLED before posting (cause={})", transactionRef, cause);
            publisher.publish(KafkaTopics.GL_EVENTS, EventType.GL_POSTING_CANCELLED,
                    new EventBodies.GlPostingCancelled(transactionRef, flowType, reason));
            return;
        }

        GlJournal original = found.get();
        if (original.getPostingStatus() != PostingStatus.POSTED) {
            log.info("[GL] {} posting is {} - nothing to reverse", transactionRef, original.getPostingStatus());
            return;
        }

        original.setPostingStatus(PostingStatus.REVERSED);

        GlJournal reversal = journal(transactionRef, JournalType.REVERSAL, PostingStatus.POSTED,
                original.getAmount(), original.getCurrency(), "Reversal: " + cause + " - " + reason);
        reversal.setReversalOf(original.getId());
        GlJournal savedReversal = journalRepository.save(reversal);

        List<GlJournalEntry> mirrored = entryRepository.findByJournal_Id(original.getId()).stream()
                .map(l -> line(savedReversal, l.getAccountCode(), l.getCreditAmount(), l.getDebitAmount()))  // swap Dr/Cr
                .toList();
        entryRepository.saveAll(mirrored);

        log.warn("[GL] {} REVERSED (compensation, cause={}) | reversal journal {}", transactionRef, cause, savedReversal.getId());
        publisher.publish(KafkaTopics.GL_EVENTS, EventType.GL_POSTING_REVERSED,
                new EventBodies.GlPostingReversed(transactionRef, flowType, original.getId(), savedReversal.getId(),
                        original.getAmount(), original.getCurrency(), reason));
    }

    @Transactional(readOnly = true)
    public List<GlPostingResponse> getPostings(String transactionRef) {
        List<GlJournal> journals = journalRepository.findByTransactionRefOrderByPostedAtAsc(transactionRef);
        if (journals.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "GL posting not found: " + transactionRef);
        }
        return journals.stream().map(this::toResponse).toList();
    }

    // ------------------------------------------------------------------

    private GlJournal journal(String transactionRef, JournalType type, PostingStatus status,
                              BigDecimal amount, String currency, String reason) {
        return GlJournal.builder()
                .transactionRef(transactionRef)
                .journalType(type)
                .postingStatus(status)
                .amount(amount)
                .currency(currency)
                .reason(reason)
                .build();
    }

    private GlJournalEntry line(GlJournal journal, String account, BigDecimal debit, BigDecimal credit) {
        return GlJournalEntry.builder().journal(journal).accountCode(account)
                .debitAmount(debit).creditAmount(credit).build();
    }

    private GlPostingResponse toResponse(GlJournal j) {
        List<GlPostingResponse.Entry> entries = entryRepository.findByJournal_Id(j.getId()).stream()
                .map(e -> GlPostingResponse.Entry.builder()
                        .accountCode(e.getAccountCode())
                        .debitAmount(e.getDebitAmount())
                        .creditAmount(e.getCreditAmount())
                        .build())
                .toList();
        return GlPostingResponse.builder()
                .id(j.getId())
                .transactionRef(j.getTransactionRef())
                .journalType(j.getJournalType().name())
                .postingStatus(j.getPostingStatus().name())
                .reversalOf(j.getReversalOf())
                .reason(j.getReason())
                .postedAt(j.getPostedAt())
                .entries(entries)
                .build();
    }
}
