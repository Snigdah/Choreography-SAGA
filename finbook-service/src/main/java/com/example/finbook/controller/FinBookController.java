package com.example.finbook.controller;

import com.example.finbook.dto.response.FinBookEntryResponse;
import com.example.finbook.service.FinBookService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/finbook")
@RequiredArgsConstructor
public class FinBookController {

    private final FinBookService finBookService;

    @GetMapping("/entries/{transactionRef}")
    public FinBookEntryResponse getEntry(@PathVariable String transactionRef) {
        return finBookService.getEntry(transactionRef);
    }
}
