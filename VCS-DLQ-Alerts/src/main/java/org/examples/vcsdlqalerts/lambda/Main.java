package org.examples.vcsdlqalerts.lambda;

public class Main {

    public static void main(String[] args) throws Exception {

        var convertor = new AvroToJsonConverter();

        //var avroSchema = Util.byteArrayToString(Util.loadResourceFile("avroScheme.json"));
        //Util.createAvroFile(Util.AVRO_SCHEMA_NAME);
        //convertor.registerSchema(Util.AVRO_SCHEMA_NAME, avroSchema);


        var json = convertor.avroToJson(Util.AVRO_SCHEMA_NAME, Util.loadResourceFile(Util.DATA_FILE_NAME));
        System.out.println(json);
        Util.sendToSlack(json);
    }
}