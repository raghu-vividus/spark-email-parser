package org.vividus.parser.spark_email_parser.service;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

@Component
@RequiredArgsConstructor
public class EmailWriter implements ItemWriter<Row> {

    private final SparkSession sparkSession;

    private final JavaSparkContext sc;

    @Override
    public void write(List<? extends Row> rows) throws Exception {

        if(rows.isEmpty()){
            return;
        }
        StructType schema=new StructType(new StructField[]{
                new StructField("From", DataTypes.StringType,true, Metadata.empty()),
                new StructField("To",DataTypes.StringType,true,Metadata.empty()),
                new StructField("CC",DataTypes.StringType,true,Metadata.empty()),
                new StructField("BCC",DataTypes.StringType,true,Metadata.empty()),
                new StructField("Subject",DataTypes.StringType,true,Metadata.empty()),
                new StructField("Sent-Date",DataTypes.StringType,true,Metadata.empty())
        });

        JavaRDD<Row> rowsRdd= sc.parallelize(new ArrayList<>(rows));
        //Converting to DataFrame
        Dataset<Row> emailDF=sparkSession.createDataFrame(rowsRdd,schema);

        String outputDir = "src/main/resources/sparkParsedEmailsTemp";
        String outputCsv = "src/main/resources/sparkParsedEmails.csv";

        emailDF.coalesce(1)
                .write()
                .option("header","true")
                .mode(SaveMode.Append)
                .csv(outputDir);
        /*
        emailDF.coalesce(1)
                .write().option("header","true")
                .mode(SaveMode.Append)
                .csv(outputDir);
        */
        movePartFile(outputDir,outputCsv);
    }

    public void movePartFile(String tempDir, String finalFilePath){
        File dir = new File(tempDir);
        File[] files = dir.listFiles((d, name) -> name.startsWith("part-"));

        if (files != null && files.length > 0) {
            File partFile = files[0];
            File finalFile = new File(finalFilePath);

            try {
                Files.write(finalFile.toPath(),Files.readAllBytes(partFile.toPath()), StandardOpenOption.CREATE,StandardOpenOption.APPEND);
                System.out.println("File saved as: " + finalFilePath);
            } catch (IOException e) {
                System.err.println("Error renaming part file: " + e.getMessage());
            }
        } else {
            System.err.println("No part file found in directory: " + tempDir);
        }
        for (File file : dir.listFiles()) {
            file.delete();
        }
        dir.delete();
    }
}
