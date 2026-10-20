package com.example.trade;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * On startup, finds every *.dat file in the input directory and runs the batch job for
 * each file on its own virtual thread. The concurrency limit caps how many virtual
 * threads run at once so memory stays bounded however many files there are.
 */
@Component
public class TradeFileLauncher implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(TradeFileLauncher.class);

    private final JobOperator jobOperator;
    private final Job tradeJob;
    private final Path inputDir;
    private final int maxThreads;

    public TradeFileLauncher(JobOperator jobOperator, Job tradeJob,
                             @Value("${trade.input-dir}") Path inputDir,
                             @Value("${trade.max-threads:4}") int maxThreads) {
        this.jobOperator = jobOperator;
        this.tradeJob = tradeJob;
        this.inputDir = inputDir;
        this.maxThreads = maxThreads;
    }

    @Override
    public void run(ApplicationArguments args) throws IOException, InterruptedException {
        Semaphore permits = new Semaphore(maxThreads);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
             DirectoryStream<Path> files = Files.newDirectoryStream(inputDir, "*.dat")) {
            for (Path file : files) {
                permits.acquire(); // wait here until fewer than maxThreads files are in progress
                executor.submit(() -> {
                    try {
                        runJob(file);
                    } finally {
                        permits.release();
                    }
                });
            }
        } // close() waits for all running jobs to finish
    }

    private void runJob(Path file) {
        log.info("Processing {} on {}", file, Thread.currentThread());
        try {
            JobParameters params = new JobParametersBuilder()
                    .addString("inputFile", file.toAbsolutePath().toString())
                    .addLong("startedAt", System.currentTimeMillis())
                    .toJobParameters();
            var execution = jobOperator.start(tradeJob, params);
            log.info("Finished {} with status {}", file, execution.getStatus());
        } catch (Exception e) {
            log.error("Job failed for {}", file, e);
        }
    }
}
