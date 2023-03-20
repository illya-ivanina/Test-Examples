package org.examples.vcsdlqalerts.lambda;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerTestDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerTestDemo.class);
    private final Properties appProperties;

    private final KafkaConsumer<String, Object> consumer;
    private final CachedSchemaRegistryClient client;

    public KafkaConsumerTestDemo(Properties appProperties) {
        this.appProperties = appProperties;
        this.consumer = new KafkaConsumer<>(getKafkaProperties());
        this.client = new CachedSchemaRegistryClient(appProperties.getProperty("avro_schema_registry_url"), 100);
    }
    private Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appProperties.getProperty("bootstrap_servers"));
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, appProperties.getProperty("consumer_group_id"));
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    public void consume(String topicName) throws IOException {
        log.info("Consuming from topic {}", topicName);
        consumer.subscribe(List.of(topicName));
        while(true){
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Object> record : records){
                Object value = record.value();
                byte[] valueBytes;
                if (value instanceof String) {
                    valueBytes = ((String) value).getBytes();
                } else if (value instanceof byte[]) {
                    valueBytes = (byte[]) value;
                } else {
                    throw new RuntimeException("Unexpected value type: " + value.getClass());
                }
                DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(getAvroScheme("TEST_DEMO-value"));
                Decoder decoder = DecoderFactory.get().binaryDecoder(valueBytes, null);
                GenericRecord datum = reader.read(null, decoder);
                log.info(">>> Value: " + datum);
                //log.info("Key: " + record.key() + ";     Value: " + record.value());
                //log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }
    }

    Schema getAvroScheme_(String avroSchemaName) {
        boolean returnOnlyLatest = false;
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(client.getByVersion(avroSchemaName, 1, returnOnlyLatest).getSchema());
    }

    public Schema getAvroScheme(String avroSchemaName) {
        try {
            return Util.getSchemeStringFromFile("avroSDLQSchema.avsc");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
