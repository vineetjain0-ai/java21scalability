# Java 21 Virtual Thread Scalability Test

Runs a large number of simulated blocking I/O tasks on virtual threads,
while keeping memory bounded.

## How memory is bounded

1. **Concurrency cap** – a `Semaphore` limits how many virtual threads are alive at once.
   The permit is taken *before* the thread is created, so queued work never piles up
   as unstarted threads. If `maxThreads` is not given (or is too large), it is derived
   from the max heap: `(maxHeap * heapLimit / 2) / (2 KB + payloadKb)`.
2. **Heap watchdog** – the submitter pauses while heap usage is above `heapLimit`
   of the max heap, letting the GC catch up.

## Build & run

```bash
mvn package
java -Xmx512m -jar target/java21scalability-1.0-SNAPSHOT.jar \
     tasks=1000000 maxThreads=10000 sleepMs=100 payloadKb=4 heapLimit=0.75
```

| Option       | Default         | Meaning                                          |
|--------------|-----------------|--------------------------------------------------|
| `tasks`      | 1,000,000       | Total tasks to run                               |
| `maxThreads` | derived from heap | Max virtual threads alive at once (capped to safe limit) |
| `sleepMs`    | 100             | Simulated blocking I/O time per task             |
| `payloadKb`  | 4               | Memory each task holds while blocked             |
| `heapLimit`  | 0.75            | Heap fraction at which submission is throttled   |

Use `-Xmx` to set the overall memory ceiling; the thread cap scales with it.
