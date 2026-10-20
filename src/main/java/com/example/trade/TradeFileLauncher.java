package com.example.trade;

import java.nio.file.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** On startup, runs the trade job once for all *.dat files in the input directory. */
@Component
public class TradeFileLauncher implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TradeFileLauncher.class);

    private final JobOperator jobOperator;
    private final Job tradeJob;
    private final Path inputDir;

    public TradeFileLauncher(JobOperator jobOperator, Job tradeJob,
                             @Value("${trade.input-dir}") Path inputDir) {
        this.jobOperator = jobOperator;
        this.tradeJob = tradeJob;
        this.inputDir = inputDir;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        JobParameters params = new JobParametersBuilder()
                .addString("inputDir", inputDir.toAbsolutePath().toString())
                .addLong("startedAt", System.currentTimeMillis())
                .toJobParameters();
        JobExecution execution = jobOperator.start(tradeJob, params);

        execution.getStepExecutions().stream()
                .filter(s -> s.getStepName().startsWith("fileStep"))
                .forEach(s -> log.info("{}: read={} filtered={} published={} status={}",
                        s.getExecutionContext().getString("fileName"),
                        s.getReadCount(), s.getFilterCount(), s.getWriteCount(), s.getStatus()));
        log.info("Job finished with status {}", execution.getStatus());
    }
}
