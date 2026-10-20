package com.example.trade;

import java.math.BigDecimal;
import java.time.LocalDate;

/** One line of the .dat file: tradeId|symbol|side|quantity|price|tradeDate */
public record Trade(
        String tradeId,
        String symbol,
        String side,
        long quantity,
        BigDecimal price,
        LocalDate tradeDate,
        BigDecimal notional) {
}
