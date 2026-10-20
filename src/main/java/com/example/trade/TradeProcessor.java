package com.example.trade;

import java.math.BigDecimal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.infrastructure.item.ItemProcessor;

/** Validates a trade, normalises the symbol and calculates notional. Invalid trades are filtered out. */
public class TradeProcessor implements ItemProcessor<Trade, Trade> {

    private static final Logger log = LoggerFactory.getLogger(TradeProcessor.class);

    @Override
    public Trade process(Trade trade) {
        if (trade.quantity() <= 0 || trade.price().signum() <= 0) {
            log.debug("Skipping invalid trade {}", trade.tradeId());
            return null; // returning null filters the item out
        }
        return new Trade(
                trade.tradeId(),
                trade.symbol().toUpperCase(),
                trade.side().toUpperCase(),
                trade.quantity(),
                trade.price(),
                trade.tradeDate(),
                trade.price().multiply(BigDecimal.valueOf(trade.quantity())));
    }
}
