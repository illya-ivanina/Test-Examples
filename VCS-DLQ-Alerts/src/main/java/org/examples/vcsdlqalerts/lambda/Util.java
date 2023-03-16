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

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Util {
    public static String DATA_FILE_NAME = "data.avro";
    public static String AVRO_SCHEMA_NAME = "User";
    public static String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String SLACK_WEBHOOK_URL = System.getenv("SLACK_WEBHOOK_URL");
    private static final String SLACK_CHANNEL = "#vcs-dlq-alerts-lambda-test";

    public static final ObjectWriter OBJECT_WRITER = new ObjectMapper().writer().withDefaultPrettyPrinter();

    public static byte[] loadResourceFile(String filename) throws IOException {
        var classLoader = AvroToJsonConverter.class.getClassLoader();
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
        return byteArrayToString(loadResourceFile("avroScheme.json"));
    }

    public static String sendToSlack(String message)  {

        OkHttpClient client = new OkHttpClient();
        MediaType mediaType = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(mediaType,
                "{\"text\":\"" +
                        message.replaceAll("\"", "\\\\\"") +
                        "\",\"channel\":\"" + SLACK_CHANNEL + "\"}");
        Request request = new Request.Builder()
                .url(SLACK_WEBHOOK_URL)
                .post(body)
                .build();
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected response code: " + response.code());
            }
            return "Message sent to Slack channel " + SLACK_CHANNEL + ": " + message;
        } catch (IOException e) {
            throw new RuntimeException("Failed to send message to Slack channel: " + e.getMessage(), e);
        }
    }

    /**
     * ! Run just in IDE !
     */
    public static void createAvroFile(String schemaString) throws Exception {
        var schema = new Schema.Parser().parse(schemaString);

        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, new FileOutputStream(
                AvroToJsonConverter.class.getClassLoader().getResource("").getPath() + "/" + DATA_FILE_NAME));


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
