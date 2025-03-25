package org.vividus.parser.spark_email_parser.listeners;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;
import org.vividus.parser.spark_email_parser.service.EmailWriter;


import java.io.File;

import javax.batch.api.listener.JobListener;
import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class JobMonitoringListener implements JobExecutionListener {

    private final JavaSparkContext sc;

    private final EmailWriter writer;

    private final SparkSession sparkSession;

    @Override
    public void beforeJob(JobExecution jobExecutiion) {
        System.out.println("Job is about to start");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        JavaRDD<String> linesRdd= sc.textFile("src/main/resources/sparkParsedEmails.csv");

        String header=linesRdd.first();

        JavaRDD<String> filteredLinesRdd=linesRdd.mapPartitionsWithIndex((index,iterator)->{
            List<String> list=new ArrayList<>();

            while(iterator.hasNext()){
                String line=iterator.next();
                if(line.equals(header)){
                    continue;
                }
                list.add(line);
            }

            return list.iterator();
        },true);

        StructType schema=new StructType(new StructField[]{
                new StructField("From", DataTypes.StringType,true, Metadata.empty()),
                new StructField("To",DataTypes.StringType,true,Metadata.empty()),
                new StructField("CC",DataTypes.StringType,true,Metadata.empty()),
                new StructField("BCC",DataTypes.StringType,true,Metadata.empty()),
                new StructField("Subject",DataTypes.StringType,true,Metadata.empty()),
                new StructField("Sent-Date",DataTypes.StringType,true,Metadata.empty())
        });

        JavaRDD<Row> outputRdd=filteredLinesRdd.map( line -> {
            String[] str=line.split(",");
            String from = (str[0] == null || str[0].isEmpty()|| str[0].equals("\"\"")) ? "" : str[0];
            String to = (str[1] == null || str[1].isEmpty()|| str[1].equals("\"\"")) ? "" : str[1];
            String cc = (str[2] == null || str[2].isEmpty() || str[2].equals("\"\""))? "" : str[2];
            String bcc = (str[3] == null || str[3].isEmpty() || str[3].equals("\"\""))? "" : str[3];
            String subject = (str[4] == null || str[4].isEmpty() || str[4].equals("\"\"")) ? "" : str[4];
            String sentDate = (str[5] == null || str[5].isEmpty() || str[5].equals("\"\"")) ? "" : str[5];
            return RowFactory.create(from , to , cc, bcc, subject, sentDate);
        });

        Dataset<Row> emailDF=sparkSession.createDataFrame(outputRdd,schema);

        String outputDir = "src/main/resources/sparkParsedEmailsTemp";
        String outputCsv = "src/main/resources/cleaned-spark-emails.csv";

        emailDF.coalesce(1)
                .write()
                .option("header","true")
                .mode(SaveMode.Append)
                .csv(outputDir);
        /*
        emailDF.repartition(1)
                .write()
                .option("header","true")
                .mode(SaveMode.Append)
                .csv(outputDir);
         */

        writer.movePartFile(outputDir,outputCsv);

        File file=new File("src/main/resources/sparkParsedEmails.csv");
        file.delete();
    }

}
