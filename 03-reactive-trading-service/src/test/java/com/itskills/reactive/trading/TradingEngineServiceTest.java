package com.itskills.reactive.trading;

import com.itskills.reactive.trading.domain.Order;
import com.itskills.reactive.trading.domain.OrderSide;
import com.itskills.reactive.trading.domain.OrderStatus;
import com.itskills.reactive.trading.dto.CreateOrderRequest;
import com.itskills.reactive.trading.service.TradingEngineService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class TradingEngineServiceTest {

    @Autowired
    private TradingEngineService tradingEngineService;

    @Test
    @DisplayName("Soumission d'un ordre d'achat et matching avec un ordre de vente existant")
    void testOrderSubmissionAndMatching() {
        // Ordre de vente initial à 95 000 $
        CreateOrderRequest sellOrder = new CreateOrderRequest(
                "ACC-SELLER-1",
                "BTC-USD",
                OrderSide.SELL,
                BigDecimal.valueOf(95000),
                BigDecimal.valueOf(0.5)
        );

        // Ordre d'achat à 95 500 $ (prix supérieur -> match immédiat)
        CreateOrderRequest buyOrder = new CreateOrderRequest(
                "ACC-BUYER-1",
                "BTC-USD",
                OrderSide.BUY,
                BigDecimal.valueOf(95500),
                BigDecimal.valueOf(0.5)
        );

        Mono<Order> executionFlow = tradingEngineService.submitOrder(sellOrder)
                .then(tradingEngineService.submitOrder(buyOrder));

        StepVerifier.create(executionFlow)
                .assertNext(matchedOrder -> {
                    assertThat(matchedOrder.symbol()).isEqualTo("BTC-USD");
                    assertThat(matchedOrder.status()).isEqualTo(OrderStatus.FILLED.name());
                })
                .verifyComplete();
    }
}
