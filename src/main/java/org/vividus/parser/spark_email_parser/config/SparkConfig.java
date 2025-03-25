package org.vividus.parser.spark_email_parser.config;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    private static SparkSession sparkSession;

    @Bean
    public SparkSession sparkSession(){
        System.setProperty("spark.local.dir", "G:/spark-temp");
        if(sparkSession == null || sparkSession.sparkContext().isStopped()){
            sparkSession=SparkSession.builder()
                    .appName("Spark Email Parser")
                    .master("local[*]")
                    .config("spark.driver.allowMultipleContexts", "true")
                    .getOrCreate();
        }
        return sparkSession;
    }
    @Bean
    public JavaSparkContext sparkContext(SparkSession sparkSession){
        return new JavaSparkContext(sparkSession.sparkContext());
    }
}
