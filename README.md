# Trade Batch – Spring Boot 4 + Spring Batch + Virtual Threads + Kafka

Reads `|`-separated trade files, validates them and publishes each trade to a Kafka topic.

```
input/*.dat ──► one virtual thread per file (max N at once)
                   └─► Spring Batch job:  FlatFileItemReader ─► TradeProcessor ─► KafkaItemWriter ─► topic "trades"
```

## File format

```
tradeId|symbol|side|quantity|price|tradeDate
T1001|AAPL|BUY|100|189.25|2026-10-19
```

The processor upper-cases the symbol and side, calculates `notional = price * quantity`,
and drops trades with a quantity or price of zero or less.

## Classes

| Class | Role |
|-------|------|
| `TradeFileLauncher` | On startup, finds `*.dat` files and runs the job for each one on a virtual thread. A `Semaphore` limits it to `trade.max-threads` files at a time, so memory stays bounded. |
| `TradeJobConfig` | The Spring Batch job: reader → processor → Kafka writer, in chunks of `trade.chunk-size`. |
| `TradeProcessor` | Validation and enrichment. |
| `Trade` | The trade record (sent to Kafka as JSON, keyed by `tradeId`). |

## Run

```bash
docker compose up -d          # local Kafka on localhost:9092
mvn spring-boot:run           # processes every .dat file in ./input
```

Settings are in `src/main/resources/application.yml` (`trade.input-dir`, `trade.topic`,
`trade.chunk-size`, `trade.max-threads`).

## Test

```bash
mvn test   # uses an embedded Kafka broker, no Docker needed
```
