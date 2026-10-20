# Trade Batch – Spring Boot 4 + Spring Batch + Virtual Threads + Kafka

Reads `|`-separated trade files, validates them and publishes each trade to a Kafka topic.
Files are already split upstream (and may be up to ~500 MB each); each file is processed whole.

```
input/                    one virtual thread per file (at most trade.max-threads at once)
 ├─ trades-1.dat  ──►  file-1:  read 1000 lines ─► process ─► publish to Kafka ─► next 1000 ...
 ├─ trades-2.dat  ──►  file-2:  read 1000 lines ─► process ─► publish to Kafka ─► next 1000 ...
 └─ trades-3.dat  ──►  waits until a thread is free
```

It is a single Spring Batch job with a partitioned step: `MultiResourcePartitioner` makes one
partition per `.dat` file, and each partition runs on a virtual thread. Inside that thread the
file is streamed in chunks of `trade.chunk-size` lines, so a large file is never fully in memory.
Heap use is roughly `max-threads × chunk-size` trades, independent of file size
(a 100 MB / 2.5M-trade file ran with heap flat at ~50 MB after GC).

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
| `TradeJobConfig` | The job: partition per file, virtual-thread executor with a concurrency limit, and the per-file reader → processor → Kafka writer step. |
| `TradeProcessor` | Validation and enrichment. |
| `TradeFileLauncher` | Starts the job on startup for `trade.input-dir` and logs per-file counts. |
| `Trade` | The trade record (sent to Kafka as JSON, keyed by `tradeId`). |

## Run

```bash
docker compose up -d          # local Kafka on localhost:9092
mvn spring-boot:run           # processes every .dat file in ./input
```

Settings are in `src/main/resources/application.yml`:

| Property | Default | Meaning |
|----------|---------|---------|
| `trade.input-dir` | `input` | Folder containing the `.dat` files |
| `trade.topic` | `trades` | Kafka topic |
| `trade.max-threads` | 8 | Max files processed at the same time |
| `trade.chunk-size` | 1000 | Lines read, processed and published together |

## Test

```bash
mvn test   # uses an embedded Kafka broker, no Docker needed
```
