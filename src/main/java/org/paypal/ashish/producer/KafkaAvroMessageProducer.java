package org.paypal.ashish.producer;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.paypal.ashish.CustomerGenerator;
import org.paypal.ashish.config.Config;
import org.paypal.ashish.model.Customer;

import java.util.HashMap;


public class KafkaAvroMessageProducer extends  Config {
    public static final Logger logger = LoggerFactory.getLogger(KafkaAvroMessageProducer.class);

    public static void main(String[] args) throws JsonMappingException {
        Config config = new Config();
        config.loadPropertiesFile();
        logger.warn("Properties file loaded");
        HashMap<String, Object> kafkaparams = new HashMap<>();
        kafkaparams.put("bootstrap.servers", config.properties.getProperty("KAFKA_SERVER_ADDRESS"));
        kafkaparams.put("key.serializer",  KafkaAvroSerializer.class);
        kafkaparams.put("value.serializer", KafkaAvroSerializer.class);
        kafkaparams.put("schema.registry.url",  config.properties.getProperty("SCHEMA_REGISTRY_SERVER_URL"));

        //Schema generation for customer class
        final AvroMapper avroMapper = new AvroMapper();
        final AvroSchema avroSchema = avroMapper.schemaFor(Customer.class);
        logger.warn("Schema registry is successful");
        final Producer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(kafkaparams);

        //publishing the message
        for (int c = 0; c < 100; c++) {
            final Customer customer = CustomerGenerator.getNext();
            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema.getAvroSchema());

            recordBuilder.set("name", customer.getName());
            recordBuilder.set("email", customer.getEmail());
            recordBuilder.set("contactNumber", customer.getContactNumber());

            logger.warn("Records are build");

            final GenericRecord genericRecord = recordBuilder.build();
            final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>( config.properties.getProperty("TOPIC_NAME"),
                    "customer", genericRecord);
            producer.send(producerRecord);
            logger.warn("Producing the records");
            logger.warn("Published message for customer: " + customer);
        }

    }
}

