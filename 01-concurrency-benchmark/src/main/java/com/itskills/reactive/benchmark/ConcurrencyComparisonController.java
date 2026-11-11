package com.itskills.reactive.benchmark;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

@RestController
@RequestMapping("/api/benchmark")
public class ConcurrencyComparisonController {

    /**
     * Approche NON-BLOQUANTE (WebFlux + Netty Event Loop) :
     * Le délai libère immédiatement le thread Netty de l'Event Loop.
     * Le thread peut traiter des milliers d'autres requêtes pendant l'attente.
     */
    @GetMapping("/reactive")
    public Mono<Map<String, Object>> reactiveEndpoint(
            @RequestParam(defaultValue = "100") long delayMs) {
        long start = System.currentTimeMillis();

        return Mono.delay(Duration.ofMillis(delayMs))
                .map(tick -> Map.of(
                        "mode", "REACTIVE_NON_BLOCKING",
                        "durationMs", System.currentTimeMillis() - start,
                        "thread", Thread.currentThread().getName(),
                        "isVirtual", Thread.currentThread().isVirtual(),
                        "threadGroup", Thread.currentThread().getThreadGroup().getName()
                ));
    }

    /**
     * ANTI-PATTERN MORTEL en WebFlux :
     * Bloquer le thread de l'Event Loop avec un sleep ou un appel synchrone bloquant.
     * Avec Netty (qui alloue seulement 1 thread par cœur CPU par défaut),
     * quelques requêtes suffisent à geler complètement tout le serveur !
     */
    @GetMapping("/blocking-anti-pattern")
    public Mono<Map<String, Object>> blockingAntiPattern(
            @RequestParam(defaultValue = "100") long delayMs) {
        long start = System.currentTimeMillis();

        return Mono.fromCallable(() -> {
            try {
                // Bloque physiquement le thread Netty !
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return Map.of(
                    "mode", "BLOCKING_ANTI_PATTERN",
                    "durationMs", System.currentTimeMillis() - start,
                    "thread", Thread.currentThread().getName(),
                    "isVirtual", Thread.currentThread().isVirtual(),
                    "warning", "NE JAMAIS BLOQUER L'EVENT LOOP NETTY EN PRODUCTION"
            );
        });
    }

    /**
     * Inspection du Thread courant et des compteurs JVM.
     */
    @GetMapping("/thread-info")
    public Mono<Map<String, Object>> threadInfo() {
        Thread current = Thread.currentThread();
        return Mono.just(Map.of(
                "threadName", current.getName(),
                "threadId", current.threadId(),
                "isVirtual", current.isVirtual(),
                "isAlive", current.isAlive(),
                "availableProcessors", Runtime.getRuntime().availableProcessors(),
                "activeThreadCount", Thread.activeCount()
        ));
    }
}
