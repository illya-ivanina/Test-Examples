package org.examples.vcsdlqalerts.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.UserInfoCredentialProvider;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import static org.examples.vcsdlqalerts.lambda.Util.OBJECT_WRITER;

public class KafkaLambdaHandlerArchiveOld implements RequestHandler<KafkaEvent, Void> {

    private static final Logger log = LoggerFactory.getLogger(KafkaLambdaHandlerArchiveOld.class);
    private final SchemaRegistryClient client;

    public KafkaLambdaHandlerArchiveOld() {
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


    @SneakyThrows
    @Override
    public Void handleRequest(KafkaEvent event, Context context) {
        log.info(">>> I'm here. Received event: " + event);
        Map<String, ? extends Iterable<KafkaEvent.KafkaEventRecord>> records = event.getRecords();
        for (Map.Entry<String, ? extends Iterable<KafkaEvent.KafkaEventRecord>> entry : records.entrySet()) {
            String topic = entry.getKey();
            Iterable<KafkaEvent.KafkaEventRecord> topicRecords = entry.getValue();
            for (KafkaEvent.KafkaEventRecord record : topicRecords) {
                log.info(">>> Received record message: " + record.getValue());
                var messageDTO = MessageDTO.builder()
                        .message(getMessage(record.getValue()))
                        .topic(topic)
                        .partition(record.getPartition())
                        .offset(record.getOffset())
                        .build();
                Util.sendToSlack(OBJECT_WRITER.writeValueAsString(messageDTO));
            }
        }
        return null;
    }

    public Schema getSchema() throws RestClientException, IOException {
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(client.getSchemaById(Integer.parseInt(Config.getProperty("SCHEMA_ID"))).canonicalString());
    }

    public String convertAvroBytesToString(Schema schema, byte[] avroData) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        log.info(">>> Before getting ByteArrayInputStream from avroData");
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(avroData);
        DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(byteArrayInputStream, datumReader);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("{[");
        for (GenericRecord record : dataFileStream) {
            stringBuilder.append(record.toString()).append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        stringBuilder.append("]}");

        return stringBuilder.toString();
    }

    private String getMessage(String message) throws Exception {
        return "original message: " + message + "\n" +
                "converted message: " +
                //avroConvertor.avroToJson(Util.AVRO_SCHEMA_NAME, message.getBytes(StandardCharsets.UTF_8))
                //avroConvertor.convertAvroBytesToString(message.getBytes(StandardCharsets.UTF_8))
                //decode(message, logger) +
                "\n";
    }


}
