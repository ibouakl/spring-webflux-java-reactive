package com.itskills.reactive.trading.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Table("market_quotes")
public record MarketQuote(
        @Id String symbol,
        BigDecimal lastPrice,
        BigDecimal bid,
        BigDecimal ask,
        BigDecimal volume24h,
        Instant updatedAt
) {}
