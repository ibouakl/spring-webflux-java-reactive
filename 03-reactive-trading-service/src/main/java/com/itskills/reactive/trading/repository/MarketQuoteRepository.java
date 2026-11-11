package com.itskills.reactive.trading.repository;

import com.itskills.reactive.trading.domain.MarketQuote;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MarketQuoteRepository extends R2dbcRepository<MarketQuote, String> {
}
