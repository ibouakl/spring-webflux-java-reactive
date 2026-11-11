package com.itskills.reactive.trading.dto;

import com.itskills.reactive.trading.domain.Order;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderResponse(
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
    public static OrderResponse from(Order order) {
        return new OrderResponse(
                order.orderId(),
                order.accountId(),
                order.symbol(),
                order.side(),
                order.type(),
                order.price(),
                order.quantity(),
                order.status(),
                order.createdAt()
        );
    }
}
