package com.bm.springbatch;

/**
 * @PROJECT IntelliJ IDEA
 * @AUTHOR Bikash Mainali
 * @DATE 6/1/24
 */

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
@Profile("spring-boot")
public class JobConfig {
    @Value("input/record.csv")
    private Resource inputCsv;

    @Value("file:src/main/resources/output/record.csv")
    private WritableResource outputCsv;

    public ItemReader<Payment> itemReader(Resource inputData) throws UnexpectedInputException {
        FlatFileItemReader<Payment> reader = new FlatFileItemReader<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        String[] tokens = {"username", "userid", "amount"};
        tokenizer.setNames(tokens);
        reader.setResource(inputCsv);
        DefaultLineMapper<Payment> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);

        BeanWrapperFieldSetMapper<Payment> fieldSetMapper = new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Payment.class);

        lineMapper.setFieldSetMapper(fieldSetMapper);

        reader.setLinesToSkip(1);
        reader.setLineMapper(lineMapper);
        return reader;
    }

    @Bean
    public ItemProcessor<Payment, Payment> itemProcessor() {
        return new PaymentItemProcessor();
    }


    public ItemWriter<Payment> itemWriter() {
        FlatFileItemWriter<Payment> itemWriter = new FlatFileItemWriter<>();

        itemWriter.setHeaderCallback(writer -> writer.write("field1,field2,field3"));
        DelimitedLineAggregator<Payment> aggregator = new DelimitedLineAggregator();
        aggregator.setDelimiter(",");
        String[] tokens = {"username", "userId", "amount"};

        BeanWrapperFieldExtractor<Payment> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(tokens);

        aggregator.setFieldExtractor(fieldExtractor);
        itemWriter.setLineAggregator(aggregator);

        itemWriter.setResource(outputCsv);
        return itemWriter;
    }


//    @Bean(name = "transactionManager")
//    public PlatformTransactionManager getTransactionManager() {
//        return new ResourcelessTransactionManager();
//    }
//
    @Bean
    public DataSource dataSource() {
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        return builder.setType(EmbeddedDatabaseType.H2)
                .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
                .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
                .build();
    }

    //by default spring boot provides jobRepository bean. If needed you can customize with below code
//    @Bean(name = "jobRepository")
//    public JobRepository getJobRepository() throws Exception {
//        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
//        factory.setDataSource(dataSource());
//        factory.setTransactionManager(getTransactionManager());
//        factory.afterPropertiesSet();
//        return factory.getObject();
//    }

    //by default spring boot provides jobLauncher bean. If needed you can customize with below code

//    @Bean(name = "jobLauncher")
//    public JobLauncher getJobLauncher() throws Exception {
//        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
//        jobLauncher.setJobRepository(getJobRepository());
//        jobLauncher.afterPropertiesSet();
//        return jobLauncher;
//    }

    @Bean
    protected Step step1(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("step1", jobRepository)
                .<Payment, Payment> chunk(10, transactionManager)
                .reader(itemReader(inputCsv))
                .processor(itemProcessor())
                .writer(itemWriter())
                .build();
    }

    @Bean(name = "csvJob")
    public Job job(JobRepository jobRepository, @Qualifier("step1") Step step1) {
        return new JobBuilder("csvJob", jobRepository)
                .preventRestart()
                .start(step1)
                .build();
    }
}
