package com.example.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.RunIdIncrementer;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.item.ItemStream;
import org.springframework.batch.infrastructure.item.ItemStreamException;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.batch.autoconfigure.JobExecutionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.io.IOException;
import java.nio.file.Files;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }
}

@Configuration
class BatchConfiguration {

    private final int gridSize = Runtime.getRuntime().availableProcessors() / 2;
    private final String workerStepName = "worker";
    private final Resource csv;
    private final Map<Long, Integer> counters = new ConcurrentHashMap<>();
    private final Map<Long, String> threads = new ConcurrentHashMap<>();

    BatchConfiguration(@Value("file://${HOME}/Desktop/data.csv") Resource csv) {
        this.csv = csv;
    }

    @EventListener
    void handle(JobExecutionEvent execution) {
        var counter = 0;
        for (var v : this.counters.values()) {
            counter += v;
        }
        var jobExecution = execution.getJobExecution();
        var end = Objects.requireNonNull(jobExecution.getEndTime()).toEpochSecond(ZoneOffset.UTC);
        var start = Objects.requireNonNull(jobExecution.getStartTime()).toEpochSecond(ZoneOffset.UTC);
        var delta = end - start;
        IO.println("finished! we read " + counter
                + " records with the following (" + this.threads.size() + ") threads " + this.threads.values()
                + " in " + delta + " seconds");
    }

    @Bean
    SimpleAsyncTaskExecutor simpleAsyncTaskExecutor() {
        var sate = new SimpleAsyncTaskExecutor();
        sate.setConcurrencyLimit(this.gridSize);
        sate.setVirtualThreads(true);
        return sate;
    }

    /**
     * Hands every partition a disjoint slice of the file: {@code skip} lines to jump over,
     * then {@code count} lines to read.
     */
    @Bean
    Partitioner partitioner() throws IOException {
        var lines = this.countLines(this.csv);
        return gridSize -> {
            var partitions = new HashMap<String, ExecutionContext>(gridSize);
            var linesPerPartition = lines / gridSize;
            for (var partition = 0; partition < gridSize; partition++) {
                var skip = partition * linesPerPartition;
                var count = partition == gridSize - 1 ? lines - skip : linesPerPartition;
                var executionContext = new ExecutionContext();
                executionContext.putInt("skip", skip);
                executionContext.putInt("count", count);
                partitions.put("partition" + partition, executionContext);
            }
            return partitions;
        };
    }

    private int countLines(Resource csv) throws IOException {
        return Files.readAllLines(csv.getFilePath()).size();
    }

    /**
     * Step-scoped so each partition gets its own reader, positioned on its own slice. One
     * shared reader would not do: {@link FlatFileItemReader} isn't thread-safe.
     */
    @Bean
    @StepScope
    FlatFileItemReader<Double> reader(
            @Value("#{stepExecutionContext['skip']}") int skip,
            @Value("#{stepExecutionContext['count']}") int count) {
        return new FlatFileItemReaderBuilder<Double>()
                .name("reader")
                .resource(this.csv)
                .linesToSkip(skip)
                .maxItemCount(count)
                .lineMapper((line, _) -> Double.parseDouble(line))
                .build();
    }

    @Bean
    Step worker(FlatFileItemReader<Double> reader, JobRepository repository) {
        return new StepBuilder(this.workerStepName, repository)
                .<Number, Number>chunk(5 * 1_000)
                .reader(reader)
                .writer(chunk -> {
                    this.logThread();
                    var currentThread = Thread.currentThread();
                    this.counters.merge(currentThread.threadId(), chunk.getItems().size(), Integer::sum);
                })
                .build();
    }

    @Bean
    Step step(Step worker, Partitioner partitioner,
              SimpleAsyncTaskExecutor simpleAsyncTaskExecutor, JobRepository repository) {
        return new StepBuilder("s1", repository)
                .partitioner(workerStepName, partitioner)
                .step(worker)
                .gridSize(gridSize)
                .taskExecutor(simpleAsyncTaskExecutor)
                .build();
    }

    private void logThread() {
        var currentThread = Thread.currentThread();
        this.threads.putIfAbsent(currentThread.threadId(),
                "#" + currentThread.threadId() + " " + currentThread.getName());
    }

    @Bean
    Job job(Step step, JobRepository repository) {
        return new JobBuilder("j1", repository)
                .start(step)
                .incrementer(new RunIdIncrementer())
                .build();
    }
}
