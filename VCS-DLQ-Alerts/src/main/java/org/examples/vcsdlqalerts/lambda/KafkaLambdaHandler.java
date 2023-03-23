package org.examples.vcsdlqalerts.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.SneakyThrows;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;
import java.util.Map;

/**
 *
 */
public class KafkaLambdaHandler implements RequestHandler<KafkaEvent, Void> {
    private static final Logger log = LoggerFactory.getLogger(KafkaLambdaHandler.class);

    private final Deserializer deserializer;

    private final SlackHelper slackHelper;

    public KafkaLambdaHandler() {
        this.slackHelper = new SlackHelper();
        var restService = new RestService(Config.getProperty("SCHEMA_REGISTRY_HOST"));
        var props = Map.of(
                SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO",
                SchemaRegistryClientConfig.USER_INFO_CONFIG,
                Config.getProperty("SCHEMA_REGISTRY_USER") + ":" + Config.getProperty("SCHEMA_REGISTRY_PASSWORD")
        );
        var provider = new UserInfoCredentialProvider();
        provider.configure(props);
        restService.setBasicAuthCredentialProvider(provider);

        this.deserializer = new KafkaAvroDeserializer(new CachedSchemaRegistryClient(restService, 10));

    }

    @SneakyThrows
    @Override
    public Void handleRequest(KafkaEvent event, Context context) {
        deserializeAndSend(deserializer, event);
        return null;
    }

    private byte[] base64Decode(KafkaEvent.KafkaEventRecord kafkaEventRecord) {
        return Base64.getDecoder().decode(kafkaEventRecord.getValue().getBytes());
    }

    private void deserializeAndSend(Deserializer deserializer, KafkaEvent kafkaEvent) {
        kafkaEvent.getRecords().forEach((key, value) -> value.forEach(v -> {
            var data = new String(base64Decode(v));
            log.info(String.format("DESERIALIZATION - KEY: %s; \nDESERIALIZATION - Raw value: %s; \n DESERIALIZATION - base64Decode  value: %s",
                    key, v, data));
            try {
                GenericRecord rec = (GenericRecord) deserializer.deserialize(v.getValue(), base64Decode(v));
                data = rec.toString();
                log.info("DESERIALIZATION - record after deserializer.deserialize: " + rec);
            } catch (Exception e) {
                data = new String(base64Decode(v));
                log.error("Error deserializing record. Trying just base64Decode", e);
            }
            var slackMessage = MessageDTO.builder()
                    .topic(Config.getProperty("KAFKA_TOPIC"))
                    .message(data)
                    .partition(v.getPartition())
                    .offset(v.getOffset())
                    .build();
            slackHelper.sendMessage(slackMessage);
        }));
    }
}
