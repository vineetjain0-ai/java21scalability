package com.example.trade;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.support.MultiResourcePartitioner;
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
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * tradeJob
 *  └─ managerStep: one partition per *.dat file in the input directory
 *       └─ fileStep, one per file, each on its own virtual thread.
 *          The file is streamed in chunks of trade.chunk-size lines:
 *          read chunk -> process chunk -> publish chunk to Kafka -> next chunk
 */
@Configuration
public class TradeJobConfig {

    @Bean
    public Job tradeJob(JobRepository jobRepository, Step managerStep) {
        return new JobBuilder("tradeJob", jobRepository)
                .start(managerStep)
                .build();
    }

    @Bean
    public Step managerStep(JobRepository jobRepository, Step fileStep,
                            MultiResourcePartitioner filePartitioner, TaskExecutor virtualThreadExecutor) {
        return new StepBuilder("managerStep", jobRepository)
                .partitioner("fileStep", filePartitioner)
                .step(fileStep)
                .taskExecutor(virtualThreadExecutor)
                .build();
    }

    @Bean
    public Step fileStep(JobRepository jobRepository,
                         FlatFileItemReader<Trade> tradeReader,
                         KafkaItemWriter<String, Trade> tradeWriter,
                         @Value("${trade.chunk-size}") int chunkSize) {
        return new StepBuilder("fileStep", jobRepository)
                .<Trade, Trade>chunk(chunkSize)
                .reader(tradeReader)
                .processor(new TradeProcessor())
                .writer(tradeWriter)
                .build();
    }

    /** One virtual thread per file; the concurrency limit caps how many files are processed at once. */
    @Bean
    public TaskExecutor virtualThreadExecutor(@Value("${trade.max-threads}") int maxThreads) {
        SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("file-");
        executor.setVirtualThreads(true);
        executor.setConcurrencyLimit(maxThreads);
        return executor;
    }

    /** Creates one partition per .dat file, storing its URL under "fileName" in the step context. */
    @Bean
    @StepScope
    public MultiResourcePartitioner filePartitioner(@Value("#{jobParameters['inputDir']}") String inputDir)
            throws IOException {
        Resource[] files = new PathMatchingResourcePatternResolver().getResources("file:" + inputDir + "/*.dat");
        MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
        partitioner.setResources(files);
        return partitioner;
    }

    /** Step-scoped: each file thread gets its own reader, which streams the file line by line. */
    @Bean
    @StepScope
    public FlatFileItemReader<Trade> tradeReader(@Value("#{stepExecutionContext['fileName']}") String fileUrl)
            throws IOException {
        return new FlatFileItemReaderBuilder<Trade>()
                .name("tradeReader")
                .resource(new UrlResource(fileUrl))
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

    /** Step-scoped: KafkaItemWriter keeps per-chunk state, so each file thread needs its own. */
    @Bean
    @StepScope
    public KafkaItemWriter<String, Trade> tradeWriter(KafkaTemplate<String, Trade> kafkaTemplate,
                                                      @Value("${trade.topic}") String topic) {
        kafkaTemplate.setDefaultTopic(topic);
        return new KafkaItemWriterBuilder<String, Trade>()
                .kafkaTemplate(kafkaTemplate)
                .itemKeyMapper(Trade::tradeId)
                .build();
    }
}
