package com.example.vt;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Load test for Java 21 virtual threads with a hard cap on concurrency so the
 * run cannot exhaust the heap.
 *
 * <p>Memory is protected in two ways:
 * <ol>
 *   <li><b>Concurrency cap</b> - a {@link Semaphore} limits how many virtual threads
 *       exist at once. A permit is acquired <i>before</i> a thread is created, so
 *       pending work never piles up as unstarted threads either. If no cap is given,
 *       one is derived from the max heap size and an estimated per-thread cost.</li>
 *   <li><b>Heap watchdog</b> - before submitting more work, the submitter checks heap
 *       usage and pauses while it is above a threshold, giving the GC time to catch up.</li>
 * </ol>
 *
 * <p>Usage (all arguments optional, {@code key=value}):
 * <pre>
 *   java -Xmx512m -jar target/java21scalability-1.0-SNAPSHOT.jar \
 *        tasks=1000000 maxThreads=10000 sleepMs=100 payloadKb=4 heapLimit=0.75
 * </pre>
 */
public final class VirtualThreadTest {

    /** Rough heap cost of one parked virtual thread (stack chunk + Thread object + task). */
    private static final long EST_BYTES_PER_VTHREAD = 2 * 1024;

    private static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();

    record Config(long tasks, int maxThreads, long sleepMs, int payloadKb, double heapLimit) {

        static Config parse(String[] args) {
            long tasks = 1_000_000;
            int maxThreads = 0; // 0 = derive from heap
            long sleepMs = 100;
            int payloadKb = 4;
            double heapLimit = 0.75;

            for (String arg : args) {
                String[] kv = arg.split("=", 2);
                if (kv.length != 2) {
                    throw new IllegalArgumentException("Expected key=value, got: " + arg);
                }
                switch (kv[0]) {
                    case "tasks" -> tasks = Long.parseLong(kv[1]);
                    case "maxThreads" -> maxThreads = Integer.parseInt(kv[1]);
                    case "sleepMs" -> sleepMs = Long.parseLong(kv[1]);
                    case "payloadKb" -> payloadKb = Integer.parseInt(kv[1]);
                    case "heapLimit" -> heapLimit = Double.parseDouble(kv[1]);
                    default -> throw new IllegalArgumentException("Unknown option: " + kv[0]);
                }
            }
            if (heapLimit <= 0 || heapLimit >= 1) {
                throw new IllegalArgumentException("heapLimit must be between 0 and 1");
            }

            int safeMax = safeMaxThreads(payloadKb, heapLimit);
            if (maxThreads <= 0) {
                maxThreads = safeMax;
            } else if (maxThreads > safeMax) {
                System.out.printf("Requested maxThreads=%,d exceeds memory-safe limit %,d; capping.%n",
                        maxThreads, safeMax);
                maxThreads = safeMax;
            }
            return new Config(tasks, maxThreads, sleepMs, payloadKb, heapLimit);
        }

        /** Largest concurrency whose estimated footprint fits in heapLimit of the max heap. */
        private static int safeMaxThreads(int payloadKb, double heapLimit) {
            long maxHeap = Runtime.getRuntime().maxMemory();
            long perThread = EST_BYTES_PER_VTHREAD + payloadKb * 1024L;
            // Use only half of the allowed budget for live threads; the rest is headroom for GC.
            long budget = (long) (maxHeap * heapLimit / 2);
            return (int) Math.max(1, Math.min(Integer.MAX_VALUE, budget / perThread));
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Config cfg = Config.parse(args);

        System.out.printf("Java %s, %d CPUs, max heap %s%n",
                Runtime.version(), Runtime.getRuntime().availableProcessors(),
                mb(Runtime.getRuntime().maxMemory()));
        System.out.printf("tasks=%,d maxThreads=%,d sleepMs=%d payloadKb=%d heapLimit=%.0f%%%n%n",
                cfg.tasks(), cfg.maxThreads(), cfg.sleepMs(), cfg.payloadKb(), cfg.heapLimit() * 100);

        Semaphore permits = new Semaphore(cfg.maxThreads());
        AtomicInteger active = new AtomicInteger();
        AtomicInteger peakActive = new AtomicInteger();
        AtomicLong completed = new AtomicLong();
        AtomicLong failed = new AtomicLong();
        AtomicLong checksum = new AtomicLong();
        long[] peakHeap = {0};
        long[] throttleEvents = {0};

        Thread reporter = Thread.ofPlatform().daemon().name("reporter").start(() -> {
            try {
                while (true) {
                    Thread.sleep(1000);
                    System.out.printf("  completed=%,d active=%,d heapUsed=%s%n",
                            completed.get(), active.get(), mb(heapUsed()));
                }
            } catch (InterruptedException ignored) {
                // run finished
            }
        });

        long start = System.nanoTime();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (long i = 0; i < cfg.tasks(); i++) {
                waitForHeap(cfg.heapLimit(), throttleEvents);
                permits.acquire(); // blocks once maxThreads are alive

                executor.submit(() -> {
                    int now = active.incrementAndGet();
                    peakActive.accumulateAndGet(now, Math::max);
                    try {
                        checksum.addAndGet(simulateIoTask(cfg.sleepMs(), cfg.payloadKb()));
                        completed.incrementAndGet();
                    } catch (Throwable t) {
                        failed.incrementAndGet();
                    } finally {
                        active.decrementAndGet();
                        permits.release();
                    }
                });

                if ((i & 0x3FF) == 0) {
                    peakHeap[0] = Math.max(peakHeap[0], heapUsed());
                }
            }
        } // close() waits for all submitted tasks
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);
        reporter.interrupt();

        double secs = elapsed.toMillis() / 1000.0;
        System.out.println();
        System.out.println("=== Result ===");
        System.out.printf("Completed       : %,d (failed %,d)%n", completed.get(), failed.get());
        System.out.printf("Elapsed         : %.2f s%n", secs);
        System.out.printf("Throughput      : %,.0f tasks/s%n", completed.get() / Math.max(secs, 0.001));
        System.out.printf("Peak concurrency: %,d (cap %,d)%n", peakActive.get(), cfg.maxThreads());
        System.out.printf("Peak heap used  : %s of %s%n", mb(peakHeap[0]), mb(Runtime.getRuntime().maxMemory()));
        System.out.printf("Heap throttles  : %,d%n", throttleEvents[0]);
        System.out.printf("Checksum        : %d%n", checksum.get());

        // With a semaphore-bounded pool the ideal time is tasks / maxThreads * sleep.
        double ideal = Math.ceil((double) cfg.tasks() / cfg.maxThreads()) * cfg.sleepMs() / 1000.0;
        System.out.printf("Ideal time      : %.2f s (efficiency %.0f%%)%n", ideal, ideal / secs * 100);
    }

    /** Simulates a blocking I/O call that holds some memory while it waits. */
    private static long simulateIoTask(long sleepMs, int payloadKb) throws InterruptedException {
        byte[] payload = new byte[payloadKb * 1024];
        ThreadLocalRandom.current().nextBytes(payload);
        Thread.sleep(sleepMs); // virtual thread unmounts from its carrier here
        return payload[0] + payload[payload.length - 1];
    }

    /** Back-pressure: pause submission while heap usage is above the configured fraction. */
    private static void waitForHeap(double heapLimit, long[] throttleEvents) throws InterruptedException {
        long limit = (long) (Runtime.getRuntime().maxMemory() * heapLimit);
        if (heapUsed() < limit) {
            return;
        }
        throttleEvents[0]++;
        System.gc(); // hint only
        while (heapUsed() >= limit) {
            Thread.sleep(10);
        }
    }

    private static long heapUsed() {
        return MEMORY.getHeapMemoryUsage().getUsed();
    }

    private static String mb(long bytes) {
        return String.format("%,d MB", bytes / (1024 * 1024));
    }
}
