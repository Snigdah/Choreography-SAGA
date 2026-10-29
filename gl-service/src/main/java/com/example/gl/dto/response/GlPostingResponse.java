package com.example.gl.dto.response;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Data
@Builder
public class GlPostingResponse {

    private UUID id;
    private String transactionRef;
    private String journalType;
    private String postingStatus;
    private UUID reversalOf;
    private String reason;
    private LocalDateTime postedAt;
    private List<Entry> entries;

    @Data
    @Builder
    public static class Entry {
        private String accountCode;
        private BigDecimal debitAmount;
        private BigDecimal creditAmount;
    }
}
