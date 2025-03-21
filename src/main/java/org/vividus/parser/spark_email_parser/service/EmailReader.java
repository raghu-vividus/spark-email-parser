package org.vividus.parser.spark_email_parser.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Component
//@StepScope
public class EmailReader implements ItemReader<String> {
    /*
    @Value("${app.inputFilePath}")
    private String inputFilePath;
       */

    private final JavaRDD<String> rawEmailsRdd;
    private final Iterator<String> iterator;

    @Autowired
    public EmailReader(JavaSparkContext sc, SparkSession spark){

        JavaRDD<String> linesRdd=sc.textFile("src/main/resources/emails.txt");

        rawEmailsRdd=linesRdd.mapPartitions( lines -> {
            List<String> emails=new ArrayList<>();
            StringBuilder emailBuilder=new StringBuilder();

            boolean isEmail=false;

            while(lines.hasNext()){
                String line=lines.next();

                if(line.startsWith("\"allen-p/")){
                    if(emailBuilder.length()>0){
                        emails.add(emailBuilder.toString().trim());
                        emailBuilder.setLength(0);
                    }
                    isEmail=true;
                }

                if(isEmail){
                    emailBuilder.append(line).append("\n");
                }
            }

            if(emailBuilder.length()>0){
                emails.add(emailBuilder.toString().trim());
            }

            return emails.iterator();
        });
        iterator=rawEmailsRdd.collect().iterator();
    }


    @Override
    public String read() throws Exception{
        return iterator.hasNext() ? iterator.next():null;
    }
}
