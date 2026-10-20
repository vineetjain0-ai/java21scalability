package com.example.trade;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

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

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "trade.input-dir=src/test/resources/input"
})
@EmbeddedKafka(partitions = 1, topics = "trades")
class TradeBatchApplicationTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Test
    void publishesValidTradesToKafka() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(broker, "test-group", true);
        props.put("auto.offset.reset", "earliest");
        try (Consumer<String, String> consumer = new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(consumer, "trades");

            List<ConsumerRecord<String, String>> records = StreamSupport
                    .stream(KafkaTestUtils.getRecords(consumer, Duration.ofSeconds(10)).spliterator(), false)
                    .toList();

            // T1003 has quantity 0 and is filtered out by the processor
            assertThat(records).extracting(ConsumerRecord::key)
                    .containsExactly("T1001", "T1002", "T1004", "T1005");
            assertThat(records.get(1).value())
                    .contains("\"symbol\":\"MSFT\"")
                    .contains("\"notional\":21055.00");
        }
    }
}
