package org.vividus.parser.spark_email_parser.service;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.jetbrains.annotations.NotNull;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

import javax.mail.Address;
import javax.mail.Message;
import javax.mail.Session;
import javax.mail.internet.MimeMessage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

@Component
public class EmailProcessor implements ItemProcessor<String, Row> {

    @Override
    public Row process(@NotNull String rawEmail) throws Exception {
        try {
            Properties props = new Properties();
            Session session = Session.getDefaultInstance(props, null);
            InputStream is = new ByteArrayInputStream(rawEmail.getBytes());
            MimeMessage message = new MimeMessage(session, is);

            String from = addressToString(message.getFrom());
            String to = addressToString(message.getRecipients(Message.RecipientType.TO));
            String cc = addressToString(message.getRecipients(Message.RecipientType.CC));
            String bcc = addressToString(message.getRecipients(Message.RecipientType.BCC));
            String subject = message.getSubject();
            String date = (message.getSentDate() != null) ? message.getSentDate().toString() : "";
            return RowFactory.create( from, to, cc, bcc, subject, date);
        }catch(Exception e){
            return RowFactory.create("","","","","","");
        }
    }

    private String addressToString(Address[] addresses){
        if(addresses==null){
            return "";
        }
        return Arrays.stream(addresses).map(Address::toString).collect(Collectors.joining("; "));
    }
}
