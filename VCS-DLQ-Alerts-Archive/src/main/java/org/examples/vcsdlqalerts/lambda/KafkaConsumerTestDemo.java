package org.examples.vcsdlqalerts.lambda;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * AUTO_OFFSET_RESET_CONFIG:
 * earliest: automatically reset the offset to the earliest offset
 * latest: automatically reset the offset to the latest offset
 * none: throw exception to the consumer if no previous offset is found or the consumer's group
 * anything else: throw exception to the consumer.
 */
public class KafkaConsumerTestDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerTestDemo.class);

    private final KafkaConsumer<String, GenericRecord> consumer;

    public KafkaConsumerTestDemo() {
        this.consumer = new KafkaConsumer<>(getKafkaProperties());
    }

    private Properties getKafkaProperties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.getProperty("KAFKA_BOOTSTRAP_SERVERS"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Config.getProperty("GROUP_ID"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        properties.setProperty("schema.registry.url", Config.getProperty("SCHEMA_REGISTRY_HOST"));
        properties.setProperty("schema.registry.basic.auth.credentials.source", "USER_INFO");
        properties.setProperty("schema.registry.basic.auth.user.info",
                Config.getProperty("SCHEMA_REGISTRY_USER") + ":" + Config.getProperty("SCHEMA_REGISTRY_PASSWORD"));

        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.mechanism", "PLAIN");
        properties.setProperty("sasl.jaas.config", PlainLoginModule.class.getName() +
                " required username=\"" +
                Config.getProperty("KAFKA_USER") +
                "\" password=\"" +
                Config.getProperty("KAFKA_PASSWORD") +
                "\";");

        return properties;
    }

    public void consume(String topicName) {
        log.info("Consuming from topic {}", topicName);
        consumer.subscribe(List.of(topicName));
        while (true) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, GenericRecord> record : records) {
                var value = record.value();
                log.info(">>> Value: " + value);
                Util.sendToSlack(value.toString());
            }
        }
    }
}
