package com.itskills.reactive.trading.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Table("trades")
public record Trade(
        @Id Long id,
        String tradeId,
        String buyOrderId,
        String sellOrderId,
        String symbol,
        BigDecimal price,
        BigDecimal quantity,
        Instant executedAt
) {
    public static Trade createNew(String tradeId, String buyOrderId, String sellOrderId, String symbol, BigDecimal price, BigDecimal quantity) {
        return new Trade(null, tradeId, buyOrderId, sellOrderId, symbol, price, quantity, Instant.now());
    }
}
