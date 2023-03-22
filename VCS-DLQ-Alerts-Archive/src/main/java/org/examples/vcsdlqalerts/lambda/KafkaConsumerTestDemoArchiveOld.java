package org.examples.vcsdlqalerts.lambda;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * AUTO_OFFSET_RESET_CONFIG:
 * earliest: automatically reset the offset to the earliest offset
 * latest: automatically reset the offset to the latest offset
 * none: throw exception to the consumer if no previous offset is found or the consumer's group
 * anything else: throw exception to the consumer.
 */
public class KafkaConsumerTestDemoArchiveOld {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerTestDemoArchiveOld.class);

    private final KafkaConsumer<String, Object> consumer;
    private final CachedSchemaRegistryClient client;

    public KafkaConsumerTestDemoArchiveOld() {
        this.consumer = new KafkaConsumer<>(getKafkaProperties());

        var restService = new RestService(Config.getProperty("SCHEMA_REGISTRY_HOST"));
        var props = Map.of(
                SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO",
                SchemaRegistryClientConfig.USER_INFO_CONFIG,
                Config.getProperty("SCHEMA_REGISTRY_USER") + ":" + Config.getProperty("SCHEMA_REGISTRY_PASSWORD")
        );
        var provider = new UserInfoCredentialProvider();
        provider.configure(props);
        restService.setBasicAuthCredentialProvider(provider);
        this.client = new CachedSchemaRegistryClient(restService, 10);
    }

    private Properties getKafkaProperties() {
        Properties properties = new Properties();
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

    public void consume(String topicName) throws IOException, RestClientException {
        log.info("Consuming from topic {}", topicName);
        consumer.subscribe(List.of(topicName));
        while (true) {
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Object> record : records) {
                Object value = record.value();
                log.info(">>> Value: " + value);
                Util.sendToSlack(value.toString());
                /*byte[] valueBytes;
                if (value instanceof String) {
                    valueBytes = ((String) value).getBytes();
                } else if (value instanceof byte[]) {
                    valueBytes = (byte[]) value;
                } else {
                    throw new RuntimeException("Unexpected value type: " + value.getClass());
                }
                DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(getAvroScheme());
                Decoder decoder = DecoderFactory.get().binaryDecoder(valueBytes, null);
                GenericRecord datum = reader.read(null, decoder);
                log.info(">>> Value: " + datum);*/
                //log.info("Key: " + record.key() + ";     Value: " + record.value());
                //log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }
    }

    /*Schema getAvroScheme_(String avroSchemaName) {
        boolean returnOnlyLatest = false;
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(client.getByVersion(avroSchemaName, 1, returnOnlyLatest).getSchema());
    }*/

    /*public Schema getAvroScheme(String avroSchemaName) {
        try {
            return Util.getSchemeStringFromFile("avroSDLQSchema.avsc");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }*/

    public Schema getAvroScheme() throws RestClientException, IOException {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(client.getSchemaById(Integer.parseInt(Config.getProperty("SCHEMA_ID"))).canonicalString());
    }
}
