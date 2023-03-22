package org.examples.vcsdlqalerts.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import lombok.SneakyThrows;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;


import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class KafkaLambdaHandler implements RequestHandler<KafkaEvent, Void> {

    private static final Logger log = LoggerFactory.getLogger(KafkaLambdaHandler.class);
    //private final SchemaRegistryClient client;

    //private final Deserializer deserializer;

    /*public KafkaLambdaHandler() {
        *//*var restService = new RestService(Config.getProperty("SCHEMA_REGISTRY_HOST"));
        var props = Map.of(
                SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO",
                SchemaRegistryClientConfig.USER_INFO_CONFIG,
                Config.getProperty("SCHEMA_REGISTRY_USER") + ":" + Config.getProperty("SCHEMA_REGISTRY_PASSWORD")
        );
        var provider = new UserInfoCredentialProvider();
        provider.configure(props);
        restService.setBasicAuthCredentialProvider(provider);
        this.client = new CachedSchemaRegistryClient(restService, 10);*//*


        //this.deserializer = new AWSKafkaAvroDeserializer(getConfigs());  // Can't find method called configure or invoke it
        //this.deserializer = new KafkaAvroDeserializer(client);  // Unknown magic byte!: org.apache.kafka.common.errors.SerializationException

    }*/


    @SneakyThrows
    @Override
    public Void handleRequest(KafkaEvent event, Context context) {
        log.info(">>> I'm here. Received event: " + event);
        //deserializeAndSend(deserializer, event);

        event.getRecords().forEach((key, value) -> value.forEach(v -> {
            var data = new String(base64Decode(v));
            log.info(">>> base64Decode  value: " + data);
            Util.sendToSlack(data);
        }));
        return null;
    }

    /*private void deserializeAndSend(Deserializer deserializer, KafkaEvent kafkaEvent) {
        kafkaEvent.getRecords().forEach((key, value) -> value.forEach(v -> {
            log.info(">>> Raw key: " + key);
            log.info(">>> Raw value: " + v);
            log.info(">>> base64Decode  value: " + new String(base64Decode(v)));

            //GenericRecord rec = (GenericRecord) deserializer.deserialize(v.getValue(), base64Decode(v));
            //log.info(">>> Received record message: " + rec);
        }));
    }*/

    /*private Map<String, Object> getConfigs() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AWSKafkaAvroDeserializer.class.getName());
        config.put(AWSSchemaRegistryConstants.AWS_REGION, Config.getProperty("AWS_REGION"));
        config.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());

        //config.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, KafkaAvroDeserializer.class.getName());
        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,  Config.getProperty("SCHEMA_REGISTRY_URL"));
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_USER_INFO_CONFIG,
                Config.getProperty("SCHEMA_REGISTRY_USER") + ":" + Config.getProperty("SCHEMA_REGISTRY_PASSWORD"));
        config.put(KafkaAvroDeserializerConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");

        //------------------------------------------------------
        *//*config.put("schema.registry.basic.auth.credentials.source", "USER_INFO");
        config.put("schema.registry.basic.auth.user.info",
                Config.getProperty("SCHEMA_REGISTRY_USER") + ":" + Config.getProperty("SCHEMA_REGISTRY_PASSWORD"));

        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config", PlainLoginModule.class.getName() +
                " required username=\"" +
                Config.getProperty("KAFKA_USER") +
                "\" password=\"" +
                Config.getProperty("KAFKA_PASSWORD") +
                "\";");*//*
        return config;
    }*/



    private byte[] base64Decode(KafkaEvent.KafkaEventRecord kafkaEventRecord) {
        return Base64.getDecoder().decode(kafkaEventRecord.getValue().getBytes());
    }


}
