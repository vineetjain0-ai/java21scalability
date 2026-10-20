package com.example.trade;

import java.math.BigDecimal;
import java.time.LocalDate;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.kafka.KafkaItemWriter;
import org.springframework.batch.infrastructure.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.kafka.core.KafkaTemplate;

@Configuration
public class TradeJobConfig {

    @Bean
    public Job tradeJob(JobRepository jobRepository, Step tradeStep) {
        return new JobBuilder("tradeJob", jobRepository)
                .start(tradeStep)
                .build();
    }

    @Bean
    public Step tradeStep(JobRepository jobRepository,
                          FlatFileItemReader<Trade> tradeReader,
                          KafkaItemWriter<String, Trade> tradeWriter,
                          @Value("${trade.chunk-size:100}") int chunkSize) {
        return new StepBuilder("tradeStep", jobRepository)
                .<Trade, Trade>chunk(chunkSize)
                .reader(tradeReader)
                .processor(new TradeProcessor())
                .writer(tradeWriter)
                .build();
    }

    /** Reads the file passed in the "inputFile" job parameter. Step-scoped so each file gets its own reader. */
    @Bean
    @StepScope
    public FlatFileItemReader<Trade> tradeReader(@Value("#{jobParameters['inputFile']}") String inputFile) {
        return new FlatFileItemReaderBuilder<Trade>()
                .name("tradeReader")
                .resource(new FileSystemResource(inputFile))
                .delimited()
                .delimiter("|")
                .names("tradeId", "symbol", "side", "quantity", "price", "tradeDate")
                .fieldSetMapper(fs -> new Trade(
                        fs.readString("tradeId"),
                        fs.readString("symbol"),
                        fs.readString("side"),
                        fs.readLong("quantity"),
                        fs.readBigDecimal("price"),
                        LocalDate.parse(fs.readString("tradeDate")),
                        BigDecimal.ZERO))
                .build();
    }

    /** Publishes each trade to Kafka, keyed by tradeId. */
    @Bean
    public KafkaItemWriter<String, Trade> tradeWriter(KafkaTemplate<String, Trade> kafkaTemplate,
                                                      @Value("${trade.topic}") String topic) {
        kafkaTemplate.setDefaultTopic(topic);
        return new KafkaItemWriterBuilder<String, Trade>()
                .kafkaTemplate(kafkaTemplate)
                .itemKeyMapper(Trade::tradeId)
                .build();
    }
}
