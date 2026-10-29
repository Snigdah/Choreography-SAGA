package com.example.compliance.controller;

import com.example.compliance.dto.response.ComplianceCheckResponse;
import com.example.compliance.service.ComplianceService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/compliance")
@RequiredArgsConstructor
public class ComplianceController {

    private final ComplianceService complianceService;

    @GetMapping("/checks/{transactionRef}")
    public ComplianceCheckResponse getCheck(@PathVariable String transactionRef) {
        return complianceService.getCheck(transactionRef);
    }
}
