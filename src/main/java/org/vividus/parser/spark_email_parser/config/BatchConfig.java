package org.vividus.parser.spark_email_parser.config;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.vividus.parser.spark_email_parser.service.EmailProcessor;
import org.vividus.parser.spark_email_parser.service.EmailReader;
import org.vividus.parser.spark_email_parser.service.EmailWriter;

@Configuration
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job emailProcessingJob(Step emailProcessingStep){

        return jobBuilderFactory.get("emailProcessingJob")
                .incrementer(new RunIdIncrementer())
                .start(emailProcessingStep)
                .build();

    }

    @Bean
    public Step processEmailsStep(EmailReader emailReader,
                                  EmailProcessor emailProcessor,
                                  EmailWriter emailWriter) {
        return stepBuilderFactory.get("emailProcessingStep")
                .<String, Row>chunk(10)
                .reader(emailReader)
                .processor(emailProcessor)
                .writer(emailWriter)
                .build();
    }
}
