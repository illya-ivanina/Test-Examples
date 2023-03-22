package org.examples.vcsdlqalerts.lambda;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import okhttp3.*;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Util {

    private static final Logger log = LoggerFactory.getLogger(Util.class);
    public static final String DATA_FILE_NAME = "VCSDLQ.avro";
    public static final String AVRO_SCHEMA_NAME = System.getenv("AVRO_SCHEMA_NAME"); //"User";
    public static final String AVRO_SCHEMA_FILE = System.getenv("AVRO_SCHEMA_FILE"); //"avroSDLQSchema.avsc";
    public static final String AVRO_SCHEMA_REGISTRY_URL = System.getenv("AVRO_SCHEMA_REGISTRY_URL"); // "http://localhost:8081";
    private static final String SLACK_WEBHOOK_URL = System.getenv("SLACK_WEBHOOK_URL");
    private static final String SLACK_CHANNEL = System.getenv("SLACK_CHANNEL"); // "#vcs-dlq-alerts-lambda-test";

    public static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer().withDefaultPrettyPrinter();

    public static byte[] loadResourceFile(String filename) throws IOException {
        var classLoader = AvroToJsonConverterDraft.class.getClassLoader();
        log.info(">>> start to load resource file: {}", filename);
        var inputStream = classLoader.getResourceAsStream(filename);
        return inputStream.readAllBytes();
    }

    public static String byteArrayToString(byte[] byteArray) {
        return new String(byteArray, StandardCharsets.UTF_8);
    }

    public static Schema getSchemeFromString(String schemaString) {
        return new Schema.Parser().parse(schemaString);
    }

    public static String getSchemeStringFromFile() throws Exception {
        return byteArrayToString(loadResourceFile(AVRO_SCHEMA_FILE));
    }

    public static Schema getSchemeStringFromFile(String avroSchemeFileName) throws Exception {
        return getSchemeFromString(byteArrayToString(loadResourceFile(avroSchemeFileName)));
    }

    public static void sendToSlack(String message)  {

        OkHttpClient client = new OkHttpClient();
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(mediaType,
                "{\"text\":\"" +
                        message.replaceAll("\"", "\\\\\"") +
                        "\",\"channel\":\"" + Config.getProperty("SLACK_CHANNEL") + "\"}");
        Request request = new Request.Builder()
                .url(Config.getProperty("SLACK_WEBHOOK_URL"))
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected response code: " + response.code());
            }
            log.info("Message sent to Slack channel " + Config.getProperty("SLACK_CHANNEL") + ": " + message);
        } catch (IOException e) {
            throw new RuntimeException("Failed to send message to Slack channel: " + e.getMessage(), e);
        }
    }

    public static Properties loadAppProperties()  {
        String resourceName = "app.properties";
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties props = new Properties();
        try(InputStream resourceStream = loader.getResourceAsStream(resourceName)) {
            props.load(resourceStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return props;
    }

    /**
     * ! Run just in IDE !
     */
    public static void createAvroFile(String schemaString) throws Exception {
        var schema = new Schema.Parser().parse(schemaString);

        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, new FileOutputStream(
                AvroToJsonConverterDraft.class.getClassLoader().getResource("").getPath() + "/" + DATA_FILE_NAME));


        var genericRecord = new GenericData.Record(schema);
        genericRecord.put("name", "John");
        genericRecord.put("age", 30);
        dataFileWriter.append(genericRecord);
        genericRecord = new GenericData.Record(schema);
        genericRecord.put("name", "Smith");
        genericRecord.put("age", 44);
        dataFileWriter.append(genericRecord);

        dataFileWriter.close();
    }
}
