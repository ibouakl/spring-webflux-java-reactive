package com.itskills.reactive.trading.controller;

import com.itskills.reactive.trading.domain.MarketQuote;
import com.itskills.reactive.trading.repository.MarketQuoteRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.accept;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

/**
 * Démonstration des Routes Fonctionnelles (Functional Endpoints) en Spring WebFlux.
 * Modèle programmatique léger sans réflexion ni annotations @Controller.
 */
@Configuration
public class FunctionalRouterConfig {

    @Bean
    public RouterFunction<ServerResponse> marketQuotesRoutes(MarketQuoteRepository repository) {
        return route(GET("/api/functional/quotes").and(accept(MediaType.APPLICATION_JSON)),
                request -> ServerResponse.ok()
                        .contentType(MediaType.APPLICATION_JSON)
                        .body(repository.findAll(), MarketQuote.class)
        ).andRoute(GET("/api/functional/quotes/{symbol}").and(accept(MediaType.APPLICATION_JSON)),
                request -> {
                    String symbol = request.pathVariable("symbol");
                    return repository.findById(symbol)
                            .flatMap(quote -> ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).bodyValue(quote))
                            .switchIfEmpty(ServerResponse.notFound().build());
                }
        );
    }
}
