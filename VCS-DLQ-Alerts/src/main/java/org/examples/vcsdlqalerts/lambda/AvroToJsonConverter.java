package org.examples.vcsdlqalerts.lambda;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.awt.image.DataBuffer;
import java.io.*;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class AvroToJsonConverter {
    private final String schemaRegistryUrl;
    private final String avroFileName;
    private final SchemaRegistryClient client;

    public AvroToJsonConverter(String schemaRegistryUrl, String avroFilePath) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.avroFileName = avroFilePath;
        this.client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }

    public AvroToJsonConverter() {
        this.schemaRegistryUrl = "http://localhost:8081";
        this.avroFileName = "data.avro";
        this.client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
    }

    public void registerSchema(String subjectName, String schemaString) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaString);
        client.register(subjectName, schema);
    }

    public void createAvroFile(String schemaString) throws Exception {
        // Define the Avro schema
        Schema schema = new Schema.Parser().parse(schemaString);

        // Create a data file writer
        DatumWriter<GenericRecord> datumWriter = new SpecificDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, new FileOutputStream("C:\\Users\\Illya_Ivanina\\WORKSPACE\\Examples\\VCS-DLQ-Alerts\\src\\main\\resources\\data.avro"));

        // Write data to the Avro file
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "John");
        record.put("age", 30);
        dataFileWriter.append(record);
        record = new GenericData.Record(schema);
        record.put("name", "Smith");
        record.put("age", 44);
        dataFileWriter.append(record);

        // Close the data file writer
        dataFileWriter.close();
    }

    public String avroToJson(String schemaName) throws Exception {
        var avroFile = Util.loadResourceFile(avroFileName);
        return avroToJson(schemaName, avroFile);
    }

    public String avroToJson(String schemaName, File avroFile) throws Exception {
        var schema = getSchema(schemaName);
        //return avroToJson(schema, Files.readAllBytes(avroFile.toPath()));
        //return deSerializeAvroHttpRequestJSON(schema, Files.readAllBytes(avroFile.toPath()));
        return getDeserializationUsingParsers(schema, Files.readAllBytes(avroFile.toPath()));
    }

    private String avroToJson(Schema schema, byte[] avroBinary) throws Exception {
        // byte to datum
        //var datumReader = new GenericDatumReader<>(schema);
        var datumReader = new GenericDatumReader<>(Util.getSchemeFromString(Util.getSchemeStringFromFile()));
        var decoder = DecoderFactory.get().binaryDecoder(avroBinary, null);
        var avroDatumObject = datumReader.read(null, decoder);

        // datum to json
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            DatumWriter<Object> writer = new GenericDatumWriter<>(schema);
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos, false);
            writer.write(avroDatumObject, encoder);
            encoder.flush();
            baos.flush();
            return baos.toString(StandardCharsets.UTF_8);
        }
    }

    public String deSerializeAvroHttpRequestJSON(Schema schema, byte[] data) throws IOException {
        DatumReader<String> reader = new SpecificDatumReader<>(String.class);
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, new String(data));
        return reader.read(null, decoder);
    }

    public String getDeserializationUsingParsers(Schema schema, byte[] data) throws IOException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        //DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(new File("C:\\Users\\Illya_Ivanina\\WORKSPACE\\Examples\\VCS-DLQ-Alerts\\src\\main\\resources\\data.avro"), datumReader);

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
        DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(byteArrayInputStream, datumReader);

        for (GenericRecord record : dataFileStream) {
            System.out.println(record);
        }


        /*GenericRecord emp = null;

        while (dataFileReader.hasNext()) {
            emp = dataFileReader.next(emp);
            System.out.println(emp);
        }*/
        return null;
    }

    private Schema getSchema(String schemaName) {
        int schemaVersion = 1;
        boolean returnOnlyLatest = false;
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(client.getByVersion(schemaName, schemaVersion, returnOnlyLatest).getSchema());
    }


}
