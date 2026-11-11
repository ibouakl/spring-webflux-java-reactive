package com.itskills.reactive.trading.repository;

import com.itskills.reactive.trading.domain.Trade;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface TradeRepository extends R2dbcRepository<Trade, Long> {

    Flux<Trade> findBySymbolOrderByExecutedAtDesc(String symbol);
}
