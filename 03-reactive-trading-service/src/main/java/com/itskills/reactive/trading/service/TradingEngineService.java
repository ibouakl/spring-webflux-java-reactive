package com.itskills.reactive.trading.service;

import com.itskills.reactive.trading.domain.Order;
import com.itskills.reactive.trading.domain.OrderSide;
import com.itskills.reactive.trading.domain.OrderStatus;
import com.itskills.reactive.trading.domain.Trade;
import com.itskills.reactive.trading.dto.CreateOrderRequest;
import com.itskills.reactive.trading.repository.OrderRepository;
import com.itskills.reactive.trading.repository.TradeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.UUID;

@Service
public class TradingEngineService {

    private static final Logger log = LoggerFactory.getLogger(TradingEngineService.class);

    private final OrderRepository orderRepository;
    private final TradeRepository tradeRepository;

    // Hot stream réactif pour diffuser en direct chaque Trade exécuté (Server-Sent Events)
    private final Sinks.Many<Trade> tradeSink = Sinks.many().multicast().onBackpressureBuffer();

    public TradingEngineService(OrderRepository orderRepository, TradeRepository tradeRepository) {
        this.orderRepository = orderRepository;
        this.tradeRepository = tradeRepository;
    }

    /**
     * Crée et persiste un ordre de manière 100% non-bloquante avec transaction réactive.
     */
    @Transactional
    public Mono<Order> submitOrder(CreateOrderRequest request) {
        String generatedOrderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        Order newOrder = Order.createNew(
                generatedOrderId,
                request.accountId(),
                request.symbol(),
                request.side(),
                request.price(),
                request.quantity()
        );

        return orderRepository.save(newOrder)
                .flatMap(this::attemptMatching)
                .doOnSuccess(saved -> log.info("Ordre soumis et enregistré : {}", saved.orderId()));
    }

    /**
     * Logique de matching réactif (Order Book) : recherche un ordre opposé compatible.
     */
    private Mono<Order> attemptMatching(Order incomingOrder) {
        String opposingSide = incomingOrder.side().equalsIgnoreCase("BUY") ? "SELL" : "BUY";

        return orderRepository.findBySymbolAndStatus(incomingOrder.symbol(), OrderStatus.PENDING.name())
                .filter(opposing -> opposing.side().equalsIgnoreCase(opposingSide))
                .filter(opposing -> isPriceCompatible(incomingOrder, opposing))
                .next() // Premier ordre compatible
                .flatMap(opposing -> executeTrade(incomingOrder, opposing))
                .defaultIfEmpty(incomingOrder);
    }

    private boolean isPriceCompatible(Order incoming, Order opposing) {
        if (incoming.side().equalsIgnoreCase("BUY")) {
            // Un acheteur est prêt à payer >= prix vendeur
            return incoming.price().compareTo(opposing.price()) >= 0;
        } else {
            // Un vendeur est prêt à vendre <= prix acheteur
            return incoming.price().compareTo(opposing.price()) <= 0;
        }
    }

    private Mono<Order> executeTrade(Order incoming, Order opposing) {
        String tradeId = "TRD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        String buyId = incoming.side().equalsIgnoreCase("BUY") ? incoming.orderId() : opposing.orderId();
        String sellId = incoming.side().equalsIgnoreCase("SELL") ? incoming.orderId() : opposing.orderId();

        Trade trade = Trade.createNew(
                tradeId,
                buyId,
                sellId,
                incoming.symbol(),
                opposing.price(), // Prix exécuté sur le carnet existant
                incoming.quantity().min(opposing.quantity())
        );

        return tradeRepository.save(trade)
                .doOnSuccess(savedTrade -> {
                    log.info("TRADE EXÉCUTÉ : {} sur {} à {}", savedTrade.tradeId(), savedTrade.symbol(), savedTrade.price());
                    tradeSink.tryEmitNext(savedTrade);
                })
                .then(orderRepository.save(opposing.withStatus(OrderStatus.FILLED)))
                .then(orderRepository.save(incoming.withStatus(OrderStatus.FILLED)));
    }

    public Flux<Order> getOrdersByAccount(String accountId) {
        return orderRepository.findByAccountId(accountId);
    }

    public Flux<Order> getPendingOrders(String symbol) {
        return orderRepository.findBySymbolAndStatus(symbol, OrderStatus.PENDING.name());
    }

    /**
     * Flux infini (Hot Stream) pour le streaming SSE en temps réel vers les clients.
     */
    public Flux<Trade> streamTrades(String symbol) {
        return tradeSink.asFlux()
                .filter(trade -> symbol == null || symbol.isBlank() || trade.symbol().equalsIgnoreCase(symbol));
    }
}
