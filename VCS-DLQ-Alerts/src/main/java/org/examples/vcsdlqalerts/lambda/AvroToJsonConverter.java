package org.examples.vcsdlqalerts.lambda;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class AvroToJsonConverter {
    private final SchemaRegistryClient client;

    public AvroToJsonConverter() {
        this.client = new CachedSchemaRegistryClient(Util.SCHEMA_REGISTRY_URL, 100);
    }

    public void registerSchema(String subjectName, String schemaString) throws Exception {
        var schema = new Schema.Parser().parse(schemaString);
        client.register(subjectName, schema);
    }


    public String avroToJson(String schemaName, byte[] avroBinary) throws Exception {
        var schema = getSchema(schemaName);
        return convertAvroBytesToString(schema, avroBinary);
    }

    private String convertAvroBytesToString(Schema schema, byte[] data) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
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

    private Schema getSchema(String schemaName) {
        int schemaVersion = 1;
        boolean returnOnlyLatest = false;
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(client.getByVersion(schemaName, schemaVersion, returnOnlyLatest).getSchema());
    }


}
