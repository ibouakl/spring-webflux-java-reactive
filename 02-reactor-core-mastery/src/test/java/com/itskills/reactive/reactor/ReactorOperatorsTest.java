package com.itskills.reactive.reactor;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReactorOperatorsTest {

    @Test
    @DisplayName("Mono : assertions sur Mono.just et Mono.empty avec StepVerifier")
    void testMonoBasics() {
        Mono<String> justMono = Mono.just("BTC-USD");

        StepVerifier.create(justMono)
                .expectNext("BTC-USD")
                .verifyComplete();

        Mono<String> emptyMono = Mono.empty();

        StepVerifier.create(emptyMono)
                .verifyComplete();
    }

    @Test
    @DisplayName("Transformation : comparaison de map vs flatMap (asynchrone)")
    void testMapVsFlatMap() {
        Flux<String> symbols = Flux.just("AAPL", "NVDA", "TSLA");

        // map : transformation synchrone 1-to-1
        Flux<String> upper = symbols.map(String::toLowerCase);

        StepVerifier.create(upper)
                .expectNext("aapl", "nvda", "tsla")
                .verifyComplete();

        // flatMap : transformation asynchrone retournant un Publisher
        Flux<String> asyncProcessed = symbols.flatMap(sym ->
                Mono.just(sym + ":TICK").delayElement(Duration.ofMillis(10))
        );

        StepVerifier.create(asyncProcessed)
                .recordWith(java.util.ArrayList::new)
                .expectNextCount(3)
                .consumeRecordedWith(results ->
                        assertThat(results).containsExactlyInAnyOrder("AAPL:TICK", "NVDA:TICK", "TSLA:TICK")
                )
                .verifyComplete();
    }

    @Test
    @DisplayName("Combinaison : zip assemble les flux élément par élément")
    void testZipOperator() {
        Flux<String> symbols = Flux.just("BTC", "ETH");
        Flux<Double> prices = Flux.just(95000.0, 3400.0);

        Flux<String> zipped = Flux.zip(symbols, prices, (s, p) -> s + "=" + p);

        StepVerifier.create(zipped)
                .expectNext("BTC=95000.0", "ETH=3400.0")
                .verifyComplete();
    }

    @Test
    @DisplayName("Filtrage & Collecte : filter puis collectList")
    void testFilterAndCollectList() {
        Flux<Integer> numbers = Flux.range(1, 10);

        Mono<List<Integer>> evens = numbers.filter(n -> n % 2 == 0).collectList();

        StepVerifier.create(evens)
                .assertNext(list -> assertThat(list).containsExactly(2, 4, 6, 8, 10))
                .verifyComplete();
    }
}
