package com.itskills.reactive.trading.repository;

import com.itskills.reactive.trading.domain.Order;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface OrderRepository extends R2dbcRepository<Order, Long> {

    Mono<Order> findByOrderId(String orderId);

    Flux<Order> findByAccountId(String accountId);

    Flux<Order> findBySymbolAndStatus(String symbol, String status);
}
