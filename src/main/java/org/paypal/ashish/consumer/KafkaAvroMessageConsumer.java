package org.paypal.ashish.consumer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.paypal.ashish.config.Config;
import org.paypal.ashish.model.Customer;
import org.paypal.ashish.producer.KafkaAvroMessageProducer;

public class KafkaAvroMessageConsumer {
    public static final Logger logger = LoggerFactory.getLogger(KafkaAvroMessageProducer.class);
    public static void main(final String[] args) {
        Config config = new Config();
        config.loadPropertiesFile();
        // Kafka Consumer Configurations
        final HashMap<String,Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  config.properties.getProperty("KAFKA_SERVER_ADDRESS"));
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,  config.properties.getProperty("CONSUMER_GROUP_ID"));
        properties.put("schema.registry.url",  config.properties.getProperty("SCHEMA_REGISTRY_SERVER_URL"));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        try(final Consumer<String, Customer> consumer = new KafkaConsumer<>(properties)){
            consumer.subscribe(Collections.singleton(config.properties.getProperty("TOPIC_NAME")));
            while (true) {
                final ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofMillis(100));

                for (final ConsumerRecord<String, Customer> record : records) {
                    logger.warn(String.format("Message Key:[ %s ] Message Payload: [ %s ]", record.key(), record.value()));
                }
                consumer.commitAsync();
            }
        }
    }


}

