# Java Avancé – Programmation Réactive avec Spring WebFlux & Project Reactor

[![Java 25](https://img.shields.io/badge/Java-25%20LTS-orange.svg)](https://adoptium.net/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-4.1.2-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Spring Framework](https://img.shields.io/badge/Spring%20Framework-7.0-green.svg)](https://spring.io/)
[![Project Reactor](https://img.shields.io/badge/Project%20Reactor-2025.0-blue.svg)](https://projectreactor.io/)
[![Docker](https://img.shields.io/badge/Docker-PostgreSQL%2016-blue.svg)](https://www.docker.com/)

Ce dépôt contient le code source complet et les travaux pratiques de la formation Udemy **« Java Avancé – Programmation Réactive avec Spring WebFlux »** animée par **Imad Bou Akl**.

---

## 🎯 Vue d'ensemble du Projet

Le projet est conçu autour d'un cas réel et exigeant : un **moteur d'ordres boursiers et de trading haute fréquence (FinTech)**.  
Il illustre concrètement pourquoi et comment l'architecture réactive non-bloquante surpasse le modèle traditionnel synchrone sous très forte charge, tout en comparant rigoureusement les **Virtual Threads de Java 21 / 25 (Projet Loom)** avec l'**Event Loop Netty de WebFlux**.

```mermaid
flowchart LR
    subgraph Clients["Clients & Microservices"]
        WebBrowser["Navigateur / SPA (EventSource SSE)"]
        ExternalAPI["Microservice Partenaire"]
    end

    subgraph WebFluxApp["03-reactive-trading-service (Netty Event Loop)"]
        Router["Functional Routers & @RestController"]
        TradingEngine["Trading Engine (Matching Orders)"]
        Sink["Sinks.Many (Hot Stream SSE)"]
        R2DBCRepo["Spring Data R2DBC Repositories"]
    end

    subgraph DataTier["Infrastructure Docker"]
        Postgres[("PostgreSQL 16 (R2DBC Non-Bloquant)")]
        Redis[("Redis 7 (Cache Réactif)")]
    end

    WebBrowser -->|GET /api/stream/quotes| Router
    ExternalAPI -->|POST /api/orders (JWT)| Router
    Router --> TradingEngine
    TradingEngine --> Sink
    Sink -.->|text/event-stream| WebBrowser
    TradingEngine --> R2DBCRepo
    R2DBCRepo -->|TCP Socket asynchrone| Postgres
```

---

## 📦 Organisation des Modules

Le projet est structuré en **3 modules Maven progressifs**, alignés avec le déroulé pédagogique du cours :

| Module | Section du Cours | Contenu & Objectifs |
|---|---|---|
| **[`01-concurrency-benchmark`](01-concurrency-benchmark)** | Section 1 : Le Déclic Réactif & La Concurrence | Comparaison pratique de charge entre le modèle synchrone *Thread-per-request*, les **Virtual Threads Java 25**, et l'**Event Loop Netty**. Illustration de la saturation de threads et de l'impact mémoire. |
| **[`02-reactor-core-mastery`](02-reactor-core-mastery)** | Section 2 : Project Reactor en Profondeur | Laboratoire d'opérateurs `Mono` & `Flux` (`map`, `flatMap`, `concatMap`, `zip`, `buffer`, `window`), gestion de résilience avec `retryWhen` et backoff exponentiel, stratégies de **Backpressure** (`limitRate`, `onBackpressureDrop`), et manipulation du temps avec `VirtualTimeScheduler`. |
| **[`03-reactive-trading-service`](03-reactive-trading-service)** | Sections 3, 4 et 5 : L'Application Complète | API FinTech haute performance : Contrôleurs réactifs, Routes Fonctionnelles (`RouterFunction`), streaming temps réel **Server-Sent Events (SSE)**, client non-bloquant `WebClient`, persistance **Spring Data R2DBC avec PostgreSQL**, sécurité réactive sans état, et tests d'intégration avec **Testcontainers**. |

---

## 🚀 Démarrage Rapide

### Prérequis
- **Java 25 LTS** (Eclipse Temurin recommandé)
- **Maven 3.9+**
- **Docker & Docker Compose** (recommandé pour PostgreSQL R2DBC)

### 1. Démarrer l'infrastructure de données locale (Docker)
À la racine du projet :
```bash
docker compose up -d
```
Cela démarre instantanément :
- **PostgreSQL 16** sur le port `5432` avec les tables (`orders`, `trades`, `market_quotes`) initialisées via `docker/init.sql`.
- **Redis 7** sur le port `6380`.

### 2. Compiler et exécuter tous les tests automatisés
```bash
mvn clean test
```
*Note : Les tests d'intégration démarrent dynamiquement un conteneur PostgreSQL via **Testcontainers** sans aucune intervention manuelle requise.*

### 3. Lancer l'application de Trading (Module 3)
```bash
cd 03-reactive-trading-service
mvn spring-boot:run
```
L'application démarre sur `http://localhost:8080`.

---

## 🧪 Scénarios de Test & Démonstration

### 1. Consommer le flux SSE en direct (Server-Sent Events)
Dans un terminal ou un navigateur :
```bash
curl -N http://localhost:8080/api/stream/quotes?symbol=BTC-USD
```
Vous recevrez un flux continu de cotations boursières non-bloquant :
```text
event:quote-tick
data:{"symbol":"BTC-USD","lastPrice":96501.20,"bid":96501.10,"ask":96501.30,"volume24h":1450000000.00,"updatedAt":"2026-11-11T16:00:00Z"}
```

### 2. Soumettre un ordre de trading (Authentification requise)
```bash
curl -X POST http://localhost:8080/api/orders \
  -u trader:trader123 \
  -H "Content-Type: application/json" \
  -d '{
    "accountId": "ACC-001",
    "symbol": "BTC-USD",
    "side": "BUY",
    "price": 96550.00,
    "quantity": 0.5
  }'
```

### 3. Consulter les routes fonctionnelles
```bash
curl http://localhost:8080/api/functional/quotes
curl http://localhost:8080/api/functional/quotes/BTC-USD
```

---

## 🛡️ Règles d'Or de la Programmation Réactive en Production

1. **Rien ne se passe tant qu'on ne souscrit pas** : Déclarer un `Mono` ou un `Flux` n'exécute rien. C'est l'abonnement (`subscribe()`, ou Spring WebFlux en retour d'endpoint) qui déclenche le flux de données.
2. **Ne JAMAIS bloquer l'Event Loop Netty** : Interdiction absolue d'appeler `.block()`, `Thread.sleep()` ou un driver JDBC synchrone sur un thread de l'Event Loop (`reactor-http-epoll-*` ou `reactor-http-nio-*`).
3. **Isoler les I/O bloquantes héritées** : Si un appel hérité bloquant est inévitable, l'encapsuler impérativement sur un thread pool dédié avec `.publishOn(Schedulers.boundedElastic())` ou `.subscribeOn(Schedulers.boundedElastic())`.
4. **Toujours dimensionner la Backpressure** : Entre un producteur rapide et un consommateur lent, définir explicitement la politique de régulation (`limitRate()`, `onBackpressureBuffer()`, `onBackpressureDrop()`).

---

## 👨‍🏫 Auteur & Licence

- **Formateur** : Imad Bou Akl
- **Chaîne & Cours** : ItSkillsMaker / Udemy
- **Licence** : Code sous licence MIT, libre d'utilisation et d'adaptation pédagogique.
