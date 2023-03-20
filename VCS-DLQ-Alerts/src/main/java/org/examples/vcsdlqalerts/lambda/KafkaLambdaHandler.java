package org.examples.vcsdlqalerts.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.examples.vcsdlqalerts.lambda.Util.OBJECT_WRITER;

public class KafkaLambdaHandler implements RequestHandler<KafkaEvent, Void> {

    private final AvroToJsonConverter avroConvertor = new AvroToJsonConverter();

    @SneakyThrows
    @Override
    public Void handleRequest(KafkaEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log(">>> I'm here. Received event: " + event);
        Map<String, ? extends Iterable<KafkaEvent.KafkaEventRecord>> records = event.getRecords();
        for (Map.Entry<String, ? extends Iterable<KafkaEvent.KafkaEventRecord>> entry : records.entrySet()) {
            String topic = entry.getKey();
            Iterable<KafkaEvent.KafkaEventRecord> topicRecords = entry.getValue();
            for (KafkaEvent.KafkaEventRecord record : topicRecords) {
                logger.log(">>> Received record message: " + record.getValue());
                var messageDTO = MessageDTO.builder()
                        .message(getMessage(record.getValue(), logger))
                        .topic(topic)
                        .partition(record.getPartition())
                        .offset(record.getOffset())
                        .build();
                Util.sendToSlack(OBJECT_WRITER.writeValueAsString(messageDTO));
            }
        }
        return null;
    }

    private String decode(String encodedMessage, LambdaLogger logger) throws Exception {
        logger.log(">>> Decoding message: " + encodedMessage);
        byte[] valueBytes = Base64.getDecoder().decode(encodedMessage);
        //byte[] valueBytes = encodedMessage.getBytes();
        Schema schema = Util.getSchemeFromString(Util.getSchemeStringFromFile());
        logger.log(">>> Scheme successfully received: " + schema.toString(false));
        DatumReader<GenericRecord> reader = new SpecificDatumReader<GenericRecord>(schema);
        logger.log(">>> The reader successfully created");
        Decoder decoder = DecoderFactory.get().binaryDecoder(valueBytes, null);
        logger.log(">>> Next: try to read the record with GenericRecord");
        GenericRecord datum = reader.read(null, decoder);
        return datum.toString();
    }

    private String getMessage(String message,LambdaLogger logger) throws Exception {
        return "original message: " + message + "\n" +
                "converted message: " +
                //avroConvertor.avroToJson(Util.AVRO_SCHEMA_NAME, message.getBytes(StandardCharsets.UTF_8))
                //avroConvertor.convertAvroBytesToString(message.getBytes(StandardCharsets.UTF_8))
                decode(message, logger) +
                "\n";
    }


}
