package com.example.trade;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "trade.max-threads=2",   // fewer threads than files, so some files wait their turn
        "trade.chunk-size=500"
})
@EmbeddedKafka(partitions = 1, topics = "trades")
class TradeBatchApplicationTest {

    private static final int FILES = 5;
    private static final int TRADES_PER_FILE = 5_000;
    private static final int TRADES = FILES * TRADES_PER_FILE;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @DynamicPropertySource
    static void inputFile(DynamicPropertyRegistry registry) throws IOException {
        Path dir = Files.createTempDirectory("trades");
        for (int f = 0; f < FILES; f++) {
            try (BufferedWriter out = Files.newBufferedWriter(dir.resolve("trades-" + f + ".dat"))) {
                for (int i = f * TRADES_PER_FILE + 1; i <= (f + 1) * TRADES_PER_FILE; i++) {
                    long quantity = i % 10 == 0 ? 0 : i; // every 10th trade is invalid
                    out.write("T" + i + "|msft|buy|" + quantity + "|421.10|2026-10-19\n");
                }
            }
        }
        registry.add("trade.input-dir", dir::toString);
    }

    @Test
    void publishesEveryValidTradeExactlyOnce() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-group", true);
        props.put("auto.offset.reset", "earliest");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, "trades");

            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            int expected = TRADES - TRADES / 10;
            long deadline = System.currentTimeMillis() + 30_000;
            while (records.size() < expected && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(records::add);
            }

            Set<String> keys = new HashSet<>();
            records.forEach(r -> keys.add(r.key()));
            assertThat(records).hasSize(expected);
            assertThat(keys).hasSize(expected).doesNotContain("T10", "T25000");
            assertThat(records).anySatisfy(r -> assertThat(r.value())
                    .contains("\"symbol\":\"MSFT\"")
                    .contains("\"notional\":"));
        }
    }
}
