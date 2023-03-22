package org.examples.vcsdlqalerts.lambda;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

public class AvroToJsonConverterDraft {
    private final SchemaRegistryClient client;


    public AvroToJsonConverterDraft()  {
        this.client = new CachedSchemaRegistryClient(Util.AVRO_SCHEMA_REGISTRY_URL, 100);

    }

    public void registerSchema(String subjectName, String schemaString) throws Exception {
        var schema = new Schema.Parser().parse(schemaString);
        client.register(subjectName, schema);
    }


    public String avroToJson(String schemaName, byte[] avroBinary) throws Exception {
        var schema = getSchema(schemaName);
        return convertAvroBytesToString(schema, avroBinary);
    }

    public String convertAvroBytesToString(Schema schema, byte[] avroData) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
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

    public String avroToJson(byte[] avroBytes) throws Exception {
        var scheme = getSchemaFromAvroBytes(avroBytes);
        return convertAvroBytesToString(scheme, avroBytes);
    }
    public Schema getSchemaFromAvroBytes(byte[] avroBytes) throws Exception {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
                new File(AvroToJsonConverterDraft.class.getClassLoader().getResource("").getPath() + "/" + Util.DATA_FILE_NAME),
                datumReader);
        Schema schema = dataFileReader.getSchema();



        return schema;
    }
    private Schema getSchema(String schemaName) {
        int schemaVersion = 1;
        boolean returnOnlyLatest = false;
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(client.getByVersion(schemaName, schemaVersion, returnOnlyLatest).getSchema());
    }


}
