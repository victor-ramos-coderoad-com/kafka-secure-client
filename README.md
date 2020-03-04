# Kafka Secure - Example
### Fast Test
1. Run zookeeper and kafka

    ```
    cd docker
    docker-comopose up -d
    ```
    
2. Run the consumer and wait the messages

    ```
    gradle runConsumer

    ```
3. Run the producer

    ```
    gradle runProducer
    
    ```
    and send messages.
    
### Details

The file <em>docker/docker-compose.yml</em> contains 2 services:

1. **Zookeeper:** It is a simple zookeeper without SSL or SASL enabled.

2. **Kafka:** A kafka container with security enabled: SSL and SASL

    - The jks files and passwords were created using <em>'docker/kafka-generate-ssl.sh'</em>
    
    - The credentials file <em>'docker/ksecure/kafka_jaas.conf'</em> contains:
    
        >user_farfetch="farfetch-secret";
        
      And it means:
      
        - user=farfetch
        
        - password=farfetch-secret
        
    - The ports defined are:
        - 9092: for inter-broker communication without security
        - 9192: for external access. SASL_SSL enabled.

3. HOST_IP is important and it's defined in <em>docker/.env</em>. 
It must contain your IP address or the domain name that the client will use to access to Kafka.

    > You must edit the java code where the Kafka servers are defined to be the same as HOST_IP. 
    

    