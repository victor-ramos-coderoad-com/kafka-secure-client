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

public class SecureConsumer {

    private boolean run = true;

    private void start(){
        String kafkaServers = System.getenv("KAFKA_SERVERS");
        String kafkaTopic = System.getenv("KAFKA_TOPIC");
        String kafkaConsumerGroup = System.getenv("KAFKA_CONSUMER_GROUP");

        String saslUser = System.getenv("SASL_JAAS_USER");
        String saslPassword = System.getenv("SASL_JAAS_PASSWORD");
        String saslEnabledMechanisms = System.getenv("SASL_ENABLED_MECHANISMS");
        String saslJaasConfig = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + saslUser + "\" password=\"" + saslPassword + "\";";

        String sslTruststoreLocation = System.getenv("SSL_TRUSTSTORE_LOCATION");
        String sslTruststorePassword = System.getenv("SSL_TRUSTSTORE_PASSWORD");
        String sslKeystoreLocation = System.getenv("SSL_KEYSTORE_LOCATION");
        String sslKeystorePassword = System.getenv("SSL_KEYSTORE_PASSWORD");
        String sslKeyPassword = System.getenv("SSL_KEY_PASSWORD");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerGroup);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, saslEnabledMechanisms);
        props.put(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststoreLocation);
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, sslKeystoreLocation);
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, sslKeystorePassword);
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, sslKeyPassword);
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        KafkaConsumer consumer = new KafkaConsumer(props);
        consumer.subscribe(Collections.singleton(kafkaTopic));
        System.out.println("Reading from " + kafkaTopic);

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
    }
}
