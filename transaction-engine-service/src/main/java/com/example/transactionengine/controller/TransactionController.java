package com.example.transactionengine.controller;

import com.example.transactionengine.dto.request.CreateTransactionRequest;
import com.example.transactionengine.dto.response.TransactionResponse;
import com.example.transactionengine.service.TransactionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor
public class TransactionController {

    private final TransactionService transactionService;

    /** Starts the Saga. Returns 202 + PENDING; poll GET to see the final status. */
    @PostMapping
    public ResponseEntity<TransactionResponse> create(@Valid @RequestBody CreateTransactionRequest request) {
        return ResponseEntity.accepted().body(transactionService.createTransaction(request));
    }

    @GetMapping("/{transactionRef}")
    public TransactionResponse get(@PathVariable String transactionRef) {
        return transactionService.getTransaction(transactionRef);
    }
}
