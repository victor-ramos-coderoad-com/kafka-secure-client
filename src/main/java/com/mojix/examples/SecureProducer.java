package com.mojix.examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SecureProducer {

    private KafkaProducer<String,String> producer;

    public SecureProducer(){
        init();
    }

    public void init(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.100.0.111:9194");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"farfetch\" password=\"farfetch-secret\";");
        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/home/vramos/data/ksecure/kafka.truststore.jks");
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "Control123!");
        props.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "/home/vramos/data/ksecure/kafka.keystore.jks");
        props.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "Control123!");
        props.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, "Control123!");
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");

        producer = new KafkaProducer<String, String>(props);
    }

    public void send(String key, String value){
        ProducerRecord<String,String> record = new ProducerRecord<>("test1",key, value);
        System.out.println("Sending");
        producer.send(record, (metadata, exception) -> {
            if(exception != null){
                System.out.println(exception.toString());
                System.out.println(exception.getCause().toString());
            }
        });
        producer.flush();
        System.out.println("Sent");
    }

    public static void main(String[] args){
        SecureProducer app = new SecureProducer();
        app.send("key1", "value1");
    }
}
