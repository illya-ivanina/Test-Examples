package org.examples.vcsdlqalerts.lambda;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

public class AvroToJsonConverter {

    private static final Logger log = LoggerFactory.getLogger(AvroToJsonConverter.class);
    private final int avroSchemaVersion;
    private final SchemaRegistryClient client;
    private final Properties appProperties;
    public AvroToJsonConverter(Properties appProperties) {
        this.appProperties = appProperties;
        var avroSchemaRegistryUrl = System.getenv("AVRO_SCHEMA_REGISTRY_URL");
        if(avroSchemaRegistryUrl == null) {
            avroSchemaRegistryUrl = appProperties.getProperty("avro_schema_registry_url");
        }
        log.info("avroSchemaRegistryUrl: {}", avroSchemaRegistryUrl);
        avroSchemaVersion = System.getenv("AVRO_SCHEMA_VERSION") != null ? Integer.parseInt(System.getenv("AVRO_SCHEMA_VERSION")) : 1;
        this.client = new CachedSchemaRegistryClient(avroSchemaRegistryUrl, 100);
        //this.client = null;
        //log.info("client is NULL right now");
    }

    public AvroToJsonConverter() {
        this(Util.loadAppProperties());
    }

    public String convertAvroBytesToString(byte[] avroData) throws Exception {
        //var schema = getSchema(appProperties.getProperty("avro_schema_name"));
        var schema = getSchemaFromFile();
        log.info(">>> schema: {}; \n {}", schema.getFullName(), schema.toString(false));
        return convertAvroBytesToString(schema, avroData);
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

    public Schema getSchema(String schemaName) {
        boolean returnOnlyLatest = false;
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(client.getByVersion(schemaName, avroSchemaVersion, returnOnlyLatest).getSchema());
    }

    public Schema getSchemaFromFile() throws Exception {
        return Util.getSchemeFromString( Util.getSchemeStringFromFile());
    }
}
