package com.mojix.examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class SecureConsumer {

    private boolean run = true;

    private void start(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.100.0.111:9192");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "SecureKafkaTest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"farfetch\" password=\"farfetch-secret\";");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, getClass().getClassLoader().getResource("kafka.truststore.jks").getPath());
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "Control123!");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, getClass().getClassLoader().getResource("kafka.keystore.jks").getPath());
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "Control123!");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "Control123!");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singleton("test_topic"));

        new Thread(() -> {
            try{
                while(run){
                    ConsumerRecords<String,String> records = consumer.poll(Duration.ofSeconds(1));
                    for(ConsumerRecord<String,String> record : records){
                        System.out.println("key="+record.key()+ " value="+record.value());
                    }
                }
            } catch (Exception e){
                e.printStackTrace();
                consumer.wakeup();
            } finally {
                consumer.wakeup();
                consumer.close();
            }

        }).start();
    }

    public static void main(String[] args){
        SecureConsumer app = new SecureConsumer();
        app.start();
        System.out.println();
        System.out.println("Reading from test_topic");
        System.out.println("Press [Enter] when you want to stop consuming");
        Scanner sc = new Scanner(System.in);
        sc.nextLine();
        app.run = false;
    }
}
