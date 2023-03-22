package org.examples.vcsdlqalerts.lambda;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Properties;

public class Main {

    public static final String dataBase64 = "AAABh0lINWUyNjdhMTEtMDQyMi00MWVhLTk4N2EtM2Q3NDdmMDI5M2UzoJGmivVgCkFEREVEDjIwMjItMDkGQ0FOZmZmZmbObUAGQ0FEFDcwMDk3OTY3NTkAAg42NjQyMDQyAgICCk9yZGVyGk9SREVSX1NISVBQRUQUMzMxNTc4ODA1MQ==";

    public static void main(String[] args) throws Exception {
        var appProperties = Util.loadAppProperties();

        //testWithConvertingAvroToJson();
        //testAvroToJsonConverterNew(appProperties);

        testWithConsuming();

    }

    static void testWithConsuming() throws IOException, RestClientException {
        var kafkaConsumerTestDemo = new KafkaConsumerTestDemo();
        kafkaConsumerTestDemo.consume(Config.getProperty("KAFKA_TOPIC"));
    }

    static void testAvroToJsonConverterNew(Properties properties){
        var converter = new AvroToJsonConverter(properties);
    }

    static void testWithConvertingAvroToJson() throws Exception {
        var convertor = new AvroToJsonConverterDraft();

        //var avroSchema = Util.byteArrayToString(Util.loadResourceFile("avroUserScheme.avsc"));
        //Util.createAvroFile(Util.AVRO_SCHEMA_NAME);
        //convertor.registerSchema(Util.AVRO_SCHEMA_NAME, avroSchema);


        var scheme = Util.getSchemeFromString(Util.getSchemeStringFromFile());
        var json = convertor.convertAvroBytesToString(scheme, Util.loadResourceFile(Util.DATA_FILE_NAME));
        //var json = convertor.avroToJson(Util.AVRO_SCHEMA_NAME, Util.loadResourceFile(Util.DATA_FILE_NAME));
        //var json = convertor.avroToJson(Util.loadResourceFile(Util.DATA_FILE_NAME));

        /*var json = convertor.convertAvroBytesToString(
                Util.getSchemeFromString(Util.getSchemeStringFromFile()) ,
                //dataBase64.getBytes(StandardCharsets.UTF_8)
                //Base64.getDecoder().decode(dataBase64.getBytes(StandardCharsets.UTF_8))
                Base64.getDecoder().decode(dataBase64)
                //new String( Base64.getDecoder().decode(dataBase64), StandardCharsets.UTF_8).getBytes()
                //new String( dataBase64.getBytes(), StandardCharsets.UTF_8).getBytes()

        );*/

        System.out.println(json);
        Util.sendToSlack(json);
    }
}