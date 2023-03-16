package org.examples.vcsdlqalerts.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import lombok.SneakyThrows;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.examples.vcsdlqalerts.lambda.Util.OBJECT_WRITER;

public class KafkaLambdaHandler implements RequestHandler<KafkaEvent, Void> {

    private final AvroToJsonConverter avroConvertor = new AvroToJsonConverter();

    @SneakyThrows
    @Override
    public Void handleRequest(KafkaEvent event, Context context) {
        LambdaLogger logger = context.getLogger();
        Map<String, ? extends Iterable<KafkaEvent.KafkaEventRecord>> records = event.getRecords();
        for (Map.Entry<String, ? extends Iterable<KafkaEvent.KafkaEventRecord>> entry : records.entrySet()) {
            String topic = entry.getKey();
            Iterable<KafkaEvent.KafkaEventRecord> topicRecords = entry.getValue();
            for (KafkaEvent.KafkaEventRecord record : topicRecords) {
                var messageDTO = MessageDTO.builder()
                        //.message(record.getValue())
                        .message(avroConvertor.avroToJson(Util.AVRO_SCHEMA_NAME, record.getValue().getBytes(StandardCharsets.UTF_8)))
                        .topic(topic)
                        .partition(record.getPartition())
                        .offset(record.getOffset())
                        .build();
                Util.sendToSlack(OBJECT_WRITER.writeValueAsString(messageDTO));
            }
        }
        return null;
    }


}
