package cn.dianhun.hadoop;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.Properties;

/**
 * SendEmail
 *
 * @author GeZhiHui
 * @create 2018-07-16
 **/

public class SendEmail {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendEmail.class);

    public static void sendEmail(String subject, String emailMsg, String email) {
        Properties props = new Properties();
        props.setProperty("mail.transport.protocol", "SMTP");
        props.setProperty("mail.smtp.host", "smtp.163.com");
        props.setProperty("mail.smtp.port", "25");

        props.setProperty("mail.smtp.auth", "true");
        props.setProperty("mail.smtp.timeout", "1000");

        Authenticator auth = new Authenticator() {
            @Override
            public PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication("18258889048@163.com", "xxxxxx");
            }
        };
        Session session = Session.getInstance(props, auth);
        Message message = new MimeMessage(session);
        try {
            message.setFrom(new InternetAddress("18258889048@163.com"));
            message.setRecipient(MimeMessage.RecipientType.TO, new InternetAddress(email));
            message.setSubject(subject);
            message.setContent(emailMsg, "text/html;charset=utf-8");
            Transport.send(message);
        } catch (MessagingException e) {
            LOGGER.error(ExceptionUtils.getFullStackTrace(e));
        }
    }

}
