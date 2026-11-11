package com.itskills.reactive.trading.domain;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;

@Table("orders")
public record Order(
        @Id Long id,
        String orderId,
        String accountId,
        String symbol,
        String side,
        String type,
        BigDecimal price,
        BigDecimal quantity,
        String status,
        Instant createdAt
) {
    public static Order createNew(String orderId, String accountId, String symbol, OrderSide side, BigDecimal price, BigDecimal quantity) {
        return new Order(
                null,
                orderId,
                accountId,
                symbol,
                side.name(),
                "LIMIT",
                price,
                quantity,
                OrderStatus.PENDING.name(),
                Instant.now()
        );
    }

    public Order withStatus(OrderStatus newStatus) {
        return new Order(id, orderId, accountId, symbol, side, type, price, quantity, newStatus.name(), createdAt);
    }
}
