package com.example.batch;

import org.aopalliance.intercept.MethodInterceptor;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.framework.ProxyFactoryBean;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.batch.infrastructure.item.ItemReader;
import org.springframework.batch.infrastructure.item.ItemWriter;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.batch.autoconfigure.JobExecutionEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

@SpringBootApplication
public class BatchApplication {

    public static void main(String[] args) {
        SpringApplication.run(BatchApplication.class, args);
    }

}


@Configuration
class BatchConfiguration {

    private final Resource input;
    private final FileSystemResource output;

    BatchConfiguration(@Value("file://${HOME}/Desktop/data.csv") Resource input,
                       @Value("file://${HOME}/Desktop/finished.csv") FileSystemResource output) {
        this.input = input;
        this.output = output;
    }

    @Bean
    Job job(JobRepository repository, Step s1) {
        return new JobBuilder("j1", repository)
                .start(s1)
                .build();
    }

    @Bean
    FlatFileItemReader<Number> reader() {
        return new FlatFileItemReaderBuilder<Number>()
                .name("reader")
                .resource(input)
                .lineMapper((line, _) -> Double.parseDouble(line))
                .build();
    }

    @Bean
    static LoggingBeanPostProcessor loggingBeanPostProcessor() {
        return new LoggingBeanPostProcessor();
    }

    static class LoggingBeanPostProcessor implements BeanPostProcessor {

        @Override
        public @Nullable Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
            if (bean instanceof ItemWriter<?> || bean instanceof ItemReader<?> || bean instanceof ItemProcessor<?, ?>)
                return threadAwareLoggingProxy(bean);
            return BeanPostProcessor.super.postProcessAfterInitialization(bean, beanName);
        }
    }

    @Bean
    FlatFileItemWriter<Number> writer() {
        return new FlatFileItemWriterBuilder<Number>()
                .resource(this.output)
                .name("w1")
                .lineSeparator(System.lineSeparator())
                .delimited(a -> a.delimiter(",").names("number").fieldExtractor(item -> new Object[]{item}))
                .build();
    }

    static final Map<Method, Collection<Thread>> THREADS = new ConcurrentHashMap<>();

    static Object threadAwareLoggingProxy(Object target) {
        var pfb = new ProxyFactoryBean();
        pfb.setTarget(target);
        for (var i : target.getClass().getInterfaces())
            pfb.addInterface(i);
        pfb.setTargetClass(target.getClass());
        pfb.setProxyTargetClass(true);
        pfb.addAdvice((MethodInterceptor) invocation -> {
            var res = invocation.getMethod().invoke(target, invocation.getArguments());
            THREADS.computeIfAbsent(invocation.getMethod(), _ -> new ConcurrentSkipListSet<>(Comparator.comparing(Thread::getName)))
                    .add(Thread.currentThread());
            return res;
        });
        return pfb.getObject();
    }

    // Step
    // readers ->(accumulate until a chunk) -> processor (chunk) -> writer

    @Bean
    Step s1(
            JobRepository repository,
            AsyncTaskExecutor asyncTaskExecutor,
            FlatFileItemReader<Number> reader,
            ItemProcessor<Number, Number> processor,
            ItemWriter<Number> writer
    ) {
        return new StepBuilder("s1", repository)
                .<Number, Number>chunk(1_000)
                .taskExecutor(asyncTaskExecutor)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    ItemProcessor<Number, Number> processor() {
        return item -> item;
    }

    @EventListener
    void on(JobExecutionEvent event) throws Exception {
        var start = event.getJobExecution().getStartTime()
                .atZone(ZoneId.systemDefault()).toEpochSecond();
        var stop = event.getJobExecution().getEndTime()
                .atZone(ZoneId.systemDefault()).toEpochSecond();

        try (var log = new BufferedWriter(new FileWriter(new File(this.input.getFile().getParentFile(), "log.txt")))) {
            THREADS.forEach((method, threads) -> {
                try {
                    log.write(method.getName() + " (" + Arrays.toString(method.getParameterTypes()) + ") = " + threads);
                }//
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        IO.println("finished " + event.getJobExecution().getJobInstance().getJobName() + " starting at " + event.getJobExecution().getStartTime()
                + " and finishing at " + event.getJobExecution().getEndTime() + " ( " + (stop - start) +
                " )");


    }

    @Bean
    SimpleAsyncTaskExecutor simpleAsyncTaskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }
}