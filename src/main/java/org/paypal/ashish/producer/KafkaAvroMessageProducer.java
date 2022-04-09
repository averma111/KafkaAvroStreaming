package org.paypal.ashish.producer;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.paypal.ashish.CustomerGenerator;
import org.paypal.ashish.model.Customer;

import java.util.HashMap;


public class KafkaAvroMessageProducer {
    public static final Logger logger = LoggerFactory.getLogger(KafkaAvroMessageProducer.class);

    private static final String TOPIC_NAME ="streamingtopic";
    private static final String KAFKA_SERVER_ADDRESS = "localhost:9092";
    private static final String AVRO_SERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroSerializer";
    private static final String SCHEMA_REGISTRY_SERVER_URL = "http://localhost:8081";

    public static void main(String[] args) throws JsonMappingException {
        HashMap<String, Object> kafkaparams = new HashMap<>();
        kafkaparams.put("bootstrap.servers", KAFKA_SERVER_ADDRESS);
        kafkaparams.put("key.serializer", AVRO_SERIALIZER_CLASS);
        kafkaparams.put("value.serializer", AVRO_SERIALIZER_CLASS);
        kafkaparams.put("schema.registry.url", SCHEMA_REGISTRY_SERVER_URL);

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
            final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(TOPIC_NAME,
                    "customer", genericRecord);
            producer.send(producerRecord);
            logger.warn("Producing the records");
            logger.warn("Published message for customer: " + customer);
        }

    }
}

