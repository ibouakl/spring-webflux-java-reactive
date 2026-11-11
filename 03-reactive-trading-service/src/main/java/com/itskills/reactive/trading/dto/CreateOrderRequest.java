package com.itskills.reactive.trading.dto;

import com.itskills.reactive.trading.domain.OrderSide;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank(message = "L'identifiant du compte est obligatoire")
        String accountId,

        @NotBlank(message = "Le symbole boursier est obligatoire (ex: BTC-USD, NVDA)")
        String symbol,

        @NotNull(message = "Le côté de l'ordre (BUY ou SELL) est obligatoire")
        OrderSide side,

        @NotNull(message = "Le prix unitaire est obligatoire")
        @Positive(message = "Le prix doit être strictement supérieur à zéro")
        BigDecimal price,

        @NotNull(message = "La quantité est obligatoire")
        @Positive(message = "La quantité doit être strictement supérieure à zéro")
        BigDecimal quantity
) {}
