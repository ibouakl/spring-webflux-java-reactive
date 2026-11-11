package com.itskills.reactive.trading.controller;

import com.itskills.reactive.trading.dto.CreateOrderRequest;
import com.itskills.reactive.trading.dto.OrderResponse;
import com.itskills.reactive.trading.service.TradingEngineService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final TradingEngineService tradingEngineService;

    public OrderController(TradingEngineService tradingEngineService) {
        this.tradingEngineService = tradingEngineService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ResponseEntity<OrderResponse>> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        return tradingEngineService.submitOrder(request)
                .map(OrderResponse::from)
                .map(res -> ResponseEntity.status(HttpStatus.CREATED).body(res));
    }

    @GetMapping("/account/{accountId}")
    public Flux<OrderResponse> getOrdersByAccount(@PathVariable String accountId) {
        return tradingEngineService.getOrdersByAccount(accountId)
                .map(OrderResponse::from);
    }

    @GetMapping("/pending/{symbol}")
    public Flux<OrderResponse> getPendingOrders(@PathVariable String symbol) {
        return tradingEngineService.getPendingOrders(symbol)
                .map(OrderResponse::from);
    }
}
